from datetime import datetime
import os
import logging
from tracemalloc import start


from airflow import DAG
# from airflow.dags.data_ingestion_gcs_dag import BIGQUERY_DATASET, PROJECT_ID
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
print(PROJECT_ID)
BUCKET = os.environ.get("GCP_GCS_BUCKET")
CREATE_PARTITION_QUERY = f"CREATE OR REPLACE TABLE {PROJECT_ID}.{BIGQUERY_DATASET}.yellow_tripdata_partitoned \
PARTITION BY \
  DATE(tpep_pickup_datetime) AS \
SELECT * FROM {PROJECT_ID}.{BIGQUERY_DATASET}.external_yellow_tripdata"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(

    dag_id='gcs_2_bq_partition_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de']

) as dag:
    gcs_2_gcs = GCSToGCSOperator(
        task_id="gcs_2_gcs",
        source_bucket=BUCKET,
        source_object='raw/*.parquet',
        destination_bucket=BUCKET,
        move_object=True,
        destination_object="yellow/"
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_yellow_trips_data",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/yellow/*"],
            },
        },
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": CREATE_PARTITION_QUERY,
                "useLegacySql": False,
            }
        },

    )

    gcs_2_gcs >> bigquery_external_table_task >> insert_query_job
