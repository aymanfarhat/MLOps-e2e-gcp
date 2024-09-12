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
Run `python src/pipeline.py`. This will generate a .json file definition of the pipeline which can then be executed either via the UI or via a script.

### Launch the pipeline
Run `python src/launch_pipeline.py`

Note: the `launch_pipeline.py` script assumes that `my_pipeline.json` exists in the src directory. You can modify the code to change it to whatever you prefer. 
