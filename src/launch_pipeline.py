import os
from dotenv import load_dotenv
from google.cloud.aiplatform import pipeline_jobs, init

load_dotenv()
project = os.getenv("PROJECT_ID")  
location = os.getenv("LOCATION")
source_table_path = os.getenv("SOURCE_TABLE_PATH")
service_account = os.getenv("SERVICE_ACCOUNT")
kafka_broker = os.getenv("KAFKA_BROKER")
kafka_topic = os.getenv("KAFKA_TOPIC")
pipline_bucket = os.getenv("PIPELINE_BUCKET")

init(project=project, location=location)

job = pipeline_jobs.PipelineJob(
    display_name="basic-pipeline",
    template_path="sample_pipeline.json",
    pipeline_root=f"gs://{pipline_bucket}/pipeline_root",
    parameter_values={
        'project_id': project,
        'location': location,
        'pipeline_bucket': pipline_bucket,
        'source_table_path': source_table_path,
        'kafka_broker': kafka_broker,
        'kafka_topic': kafka_topic
    },
    project=project,
    enable_caching=False
)

job.run(service_account=service_account)
