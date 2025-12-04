import os
from google.cloud import bigquery
from airflow.models import Variable


def load_csv_to_bq(**context):
    """Load daily sales CSV from GCS into a raw BigQuery table using LOAD job."""
    ti = context["ti"]

    config = ti.xcom_pull(task_ids="load_config", key="pipeline_config")
    project_id = config["project_id"]
    raw_dataset = config["bigquery"]["raw_dataset"]
    bucket = config["gcs"]["landing_bucket"]

    gcs_uri = f"gs://{bucket}/sales/{context['ds']}/sales_{context['ds']}.csv"
    table_id = f"{project_id}.{raw_dataset}.sales"

    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )
    load_job.result()
