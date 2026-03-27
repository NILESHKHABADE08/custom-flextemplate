from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

PROJECT_ID = "your-project-id"
REGION = "us-central1"

with DAG(
    dag_id="run_bq_to_mongo_flex",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_flex = DataflowStartFlexTemplateOperator(
        task_id="run_flex_template",
        body={
            "launchParameter": {
                "jobName": "bq-mongo-{{ ts_nodash }}",
                "containerSpecGcsPath": "gs://your-bucket/templates/bq-mongo.json",
                "parameters": {
                    "inputTable": "project:dataset.table",
                    "mongoUri": "mongodb+srv://<username>:<password>@cluster.mongodb.net"
                },
                "environment": {
                    "tempLocation": "gs://your-bucket/temp/"
                },
            }
        },
        location=REGION,
    )
