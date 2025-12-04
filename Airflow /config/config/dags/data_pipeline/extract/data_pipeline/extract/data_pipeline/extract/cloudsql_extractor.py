from sqlalchemy import create_engine, text
from google.cloud import bigquery


def extract_orders_to_bq(**context):
    """Extract incremental orders from Cloud SQL into raw.orders."""
    from airflow.hooks.base import BaseHook

    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="pipeline_config")
    window = ti.xcom_pull(task_ids="compute_incremental_window", key="incremental_window")
    project_id = config["project_id"]
    raw_dataset = config["bigquery"]["raw_dataset"]

    source_cfg = config["sources"]["cloudsql_orders"]
    conn = BaseHook.get_connection(source_cfg["cloudsql_conn_id"])

    engine = create_engine(conn.get_uri())  # composer VPC must reach Cloud SQL
    query_template = source_cfg["query_template"]
    query = query_template.replace("{{ last_loaded_ts }}", window["start_ts"])

    with engine.connect() as connection:
        rows = connection.execute(text(query)).fetchall()
        columns = rows[0].keys() if rows else []

    # Convert to list of dicts
    records = [dict(zip(columns, row)) for row in rows]

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{raw_dataset}.orders"

    errors = client.insert_rows_json(table_id, records)
    if errors:
        raise RuntimeError(f"Errors inserting Cloud SQL orders: {errors}")
