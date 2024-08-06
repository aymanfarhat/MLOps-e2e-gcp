import os
from dotenv import load_dotenv
from google.cloud.aiplatform import pipeline_jobs, init

load_dotenv()
project = os.getenv("PROJECT_ID")  
location = os.getenv("LOCATION")
source_table_path = os.getenv("SOURCE_TABLE_PATH")
service_account = os.getenv("SERVICE_ACCOUNT")

init(project=project, location=location)

job = pipeline_jobs.PipelineJob(
    display_name="basic-pipeline",
    template_path="my_pipeline.json",
    parameter_values={
        'project_id': project,
        'location': location,
        'source_table_path': source_table_path,
    },
    project=project,
    enable_caching=False
)

job.run(service_account=service_account)
