import json
import requests
from google.cloud import bigquery


def fetch_customers_to_bq(**context):
    """Call external customers API and load results into raw.customers."""
    from airflow.hooks.base import BaseHook

    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="pipeline_config")
    window = ti.xcom_pull(task_ids="compute_incremental_window", key="incremental_window")

    project_id = config["project_id"]
    raw_dataset = config["bigquery"]["raw_dataset"]
    source_cfg = config["sources"]["api_customers"]

    conn = BaseHook.get_connection(source_cfg["conn_id"])
    base_url = conn.host.rstrip("/")
    endpoint = source_cfg["endpoint"]

    # Example incremental parameter; adapt to your API
    params = {
        "updated_since": window["start_ts"],
        "updated_before": window["end_ts"],
    }

    response = requests.get(f"{base_url}{endpoint}", params=params)
    response.raise_for_status()
    records = response.json()

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{raw_dataset}.customers"

    errors = client.insert_rows_json(table_id, records)
    if errors:
        raise RuntimeError(f"Errors while inserting API customers: {errors}")
