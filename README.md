# MLOps-e2e-gcp

A template pipeline for orchestrating ML training and inference lifecycle on GCP using VertexAI pipelines, Bigframes and BigQuery. 

## Running on Vertex pipelines

### Install dependencies on the job launcher (e.g. local host)
```
pip install -r requirements.txt
```

### Setup environment variables
Create a .env file in the root directory adding the following variables. The `.env` file will be picked up by the `python-dotenv` module.

```
PROJECT_ID="[YOUR-PROJECT-ID]"
LOCATION="us-east1"
SOURCE_TABLE_PATH="[INPUT-BQ-TABLE-PATH]"
SERVICE_ACCOUNT="[SERVICE-ACCOUNT-FOR-PIPELINE]"
KAFKA_BROKER="[BROKER-IP]:[BROKER-PORT]"
KAFKA_TOPIC="test-topic"
```

### Generate a new pipeline snapshot
Run `python src/compile_pipeline.py`. This will generate a .json file definition of the pipeline which can then be executed either via the UI or via a script.

### Launch the pipeline
Run `python src/launch_pipeline.py`

Note: the `launch_pipeline.py` script assumes that `my_pipeline.json` exists in the src directory. You can modify the code to change it to whatever you prefer and to a different location.


## Service account roles and permissions

The service account used to run the pipeline must have the following roles:

- Browser
- BigQuery Connection Admin
- BigQuery Data Editor
- BigQuery Data Viewer
- BigQuery Job User
- BigQuery Read Session User
- Cloud Functions Developer
- Service Account User
- Storage Object Admin
- Vertex AI Feature Store Data Writer
- Vertex AI Feature Store Instance Creator
- Vertex AI User


The above roles can be added to the service account via the GCP console or via the command line using the `gcloud` CLI. More granular permissions can be assigned as needed as well.
