from typing import NamedTuple
from kfp.dsl import component, pipeline
from kfp import compiler, dsl


@component(
    base_image="europe-docker.pkg.dev/vertex-ai/training/tf-cpu.2-14.py310:latest",
    packages_to_install=["google-cloud-bigquery"],
)
def prepare_dataset(job_id: str, project_id: str, location: str) -> str:
    """
    Creates the dataset in BigQuery for this pipeline run.
    """
    from google.cloud import bigquery

    client = bigquery.Client()
    dataset_id = f"{project_id}.mlops_train_{job_id}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    dataset = client.create_dataset(dataset, exists_ok=True)

    return dataset_id


@component(
    base_image="europe-docker.pkg.dev/vertex-ai/training/tf-cpu.2-14.py310:latest",
    packages_to_install=["bigframes", "google-cloud-bigquery"],
)
def clean_data(
    bq_dataset_id: str, project_id: str, location: str, source_table_path: str
) -> str:
    """
    Loads data from source BQ table and applies basic cleaning operations. Writes to a new BQ table.
    """
    import bigframes.pandas as bpd

    target_table = f"{bq_dataset_id}.chicago_taxi_trips_cleaned"

    bpd.options.bigquery.project = project_id
    bpd.options.bigquery.location = location
    df = bpd.read_gbq("SELECT * FROM " + source_table_path)

    # Remove rows with missing values, duplicates, and invalid values
    df = df.dropna()
    df = df.drop_duplicates()
    df = df[df["trip_seconds"] > 0]
    df = df[df["trip_miles"] > 0]

    # data after 2018 only
    df = df[df["trip_start_timestamp"] > "2018-01-01"]
    # Limit to 1000 rows for testing
    df = df.head(10000)

    df.to_gbq(target_table, if_exists="replace")

    return target_table


@component(
    base_image="europe-docker.pkg.dev/vertex-ai/training/tf-cpu.2-14.py310:latest",
    packages_to_install=["bigframes"],
)
def prepare_features(
    bq_dataset_id: str, project_id: str, location: str, source_table_path: str
) -> str:
    """
    Loads cleaned data from BQ table and prepares features for training.
    """
    import bigframes.pandas as bpd

    target_table = f"{bq_dataset_id}.chicago_taxi_trips_features"

    bpd.options.bigquery.project = project_id
    bpd.options.bigquery.location = location
    df = bpd.read_gbq(source_table_path)

    # Feature engineering
    df["trip_start_timestamp"] = bpd.to_datetime(df["trip_start_timestamp"])
    df["trip_start_hour"] = df["trip_start_timestamp"].dt.hour
    df["trip_start_day"] = df["trip_start_timestamp"].dt.day
    df["trip_start_month"] = df["trip_start_timestamp"].dt.month
    df["trip_start_year"] = df["trip_start_timestamp"].dt.year

    @bpd.remote_function(
        bpd.Series,
        float,
        reuse=False,
        packages=["geopy"],
        bigquery_connection="bigframes-connect",
    )
    def calculate_distance(params) -> float:
        """Returns the distance between two points"""
        from geopy.distance import great_circle as GRC

        l1 = params["pickup_latitude"]
        l2 = params["pickup_longitude"]
        l3 = params["dropoff_latitude"]
        l4 = params["dropoff_longitude"]

        p1 = (l1, l2)
        p2 = (l3, l4)

        return GRC(p1, p2).km


    # Drop columns not needed for training
    df = df.drop(
        columns=[
            "trip_start_timestamp",
            "trip_end_timestamp",
            "pickup_census_tract",
            "dropoff_census_tract",
            "pickup_community_area",
            "dropoff_community_area",
            "fare",
            "tips",
            "tolls",
            "extras",
            "payment_type",
            "company",
            "taxi_id",
            "pickup_location",
            "dropoff_location",
        ]
    )

    # Calculate distance between pickup and dropoff locations
    columns = [
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
    ]

    df["distance"] = 0
    df["distance"] = df[columns].apply(calculate_distance, axis=1)

    df.to_gbq(target_table, if_exists="replace")

    return target_table


@component(
    base_image="europe-docker.pkg.dev/vertex-ai/training/tf-cpu.2-14.py310:latest",
    packages_to_install=[
        "scikit-learn",
        "pandas",
        "google-cloud-bigquery",
        "bigframes",
        "pandas-gbq",
        "google-cloud-aiplatform"
    ],
)
def train_save_model(project_id: str, location: str, source_table_path: str, model_name: str, staging_bucket: str) -> str:
    """
    Trains a model using the prepared features. Correlation is between distance and fare. Using scikit learn regression model.
    """
    import pandas
    import bigframes.pandas as bpd
    import pickle
    from sklearn import linear_model
    from google.cloud import aiplatform

    bpd.options.bigquery.project = project_id
    bpd.options.bigquery.location = location
    df_bpd = bpd.read_gbq(
        f"SELECT trip_seconds, distance, trip_total FROM {source_table_path}"
    )

    df = df_bpd.to_pandas()

    # Load data
    model = linear_model.LinearRegression()

    # Train model
    model.fit(df[["distance"]], df["trip_total"])

    model_path = "model.pkl"

    # Save model locally
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    # Upload model to Vertex AI registry
    model = aiplatform.Model.upload_scikit_learn_model_file(
        model_file_path=model_path,
        display_name=model_name,
        project=project_id,
        location=location,
        staging_bucket=staging_bucket,
    )

    return model_path


@pipeline(
    name="my-basic-pipeline",
    description="This is a basic pipeline",
    pipeline_root="gs://vertexai-demo-pipeline/pipeline_root",
)
def my_pipeline(project_id: str, location: str, source_table_path: str):
    job_run_id = dsl.PIPELINE_JOB_ID_PLACEHOLDER

    prepare_dataset_task = prepare_dataset(
        job_id=job_run_id, project_id=project_id, location=location
    )

    target_dataset = prepare_dataset_task.output
    clean_data_task = clean_data(
        bq_dataset_id=target_dataset,
        project_id=project_id,
        location=location,
        source_table_path=source_table_path,
    )

    cleaned_table = clean_data_task.output

    prepare_features_task = prepare_features(
        bq_dataset_id=target_dataset,
        project_id=project_id,
        location=location,
        source_table_path=cleaned_table,
    )

    features_table = prepare_features_task.output

    train_save_model_task = train_save_model(
        project_id=project_id,
        location=location,
        source_table_path=features_table,
        model_name="taxi_fare_model",
        staging_bucket="gs://vertexai-demo-pipeline"
    )

compiler.Compiler().compile(pipeline_func=my_pipeline, package_path="my_pipeline.json")
