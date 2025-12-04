from datetime import datetime, timedelta
import os
import yaml

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

from data_pipeline.dq.dq_operators import DataQualityOperator
from data_pipeline.extract.csv_extractor import load_csv_to_bq
from data_pipeline.extract.api_extractor import fetch_customers_to_bq
from data_pipeline.extract.cloudsql_extractor import extract_orders_to_bq
from data_pipeline.transform.bq_transform import build_aggregated_views
from data_pipeline.load.bq_loader import merge_raw_to_staging


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _load_config(**context):
    """Load YAML config and push to XCom."""
    from airflow.models import Variable

    config_path = Variable.get("PIPELINE_CONFIG_PATH", "config/pipeline_config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    context["ti"].xcom_push(key="pipeline_config", value=config)


def _get_incremental_window(**context):
    """Compute incremental window and push to XCom.

    In a real setup, you'd read/write a state table in BigQuery.
    Here we just use execution_date as a simplified example.
    """
    execution_date = context["ds"]  # YYYY-MM-DD
    # For demo, use previous day as start; in reality use state table.
    start_ts = (context["logical_date"] - timedelta(days=1)).isoformat()
    end_ts = context["logical_date"].isoformat()

    window = {"start_ts": start_ts, "end_ts": end_ts}
    context["ti"].xcom_push(key="incremental_window", value=window)


with DAG(
    dag_id="multi_source_etl",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gcp", "composer", "etl", "multi-source"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    load_config = PythonOperator(
        task_id="load_config",
        python_callable=_load_config,
    )

    compute_incremental_window = PythonOperator(
        task_id="compute_incremental_window",
        python_callable=_get_incremental_window,
    )

    # ----------------------------------------------------
    # Extract from multiple sources
    # ----------------------------------------------------
    with TaskGroup(group_id="extract") as extract_group:

        # CSV from GCS
        csv_sensor = GCSObjectsWithPrefixExistenceSensor(
            task_id="wait_for_csv_sales",
            bucket="{{ ti.xcom_pull(task_ids='load_config', key='pipeline_config')['gcs']['landing_bucket'] }}",
            prefix="sales/{{ ds }}/",
            poke_interval=300,
            timeout=3600,
        )

        extract_csv = PythonOperator(
            task_id="extract_csv_sales_to_bq",
            python_callable=load_csv_to_bq,
        )

        csv_sensor >> extract_csv

        # API customers (incremental)
        extract_api = PythonOperator(
            task_id="extract_api_customers_to_bq",
            python_callable=fetch_customers_to_bq,
        )

        # Cloud SQL orders (incremental)
        extract_cloudsql = PythonOperator(
            task_id="extract_cloudsql_orders_to_bq",
            python_callable=extract_orders_to_bq,
        )

    # ----------------------------------------------------
    # Load / merge raw -> staging
    # ----------------------------------------------------
    with TaskGroup(group_id="load") as load_group:
        merge_sales = PythonOperator(
            task_id="merge_sales_raw_to_staging",
            python_callable=merge_raw_to_staging,
            op_kwargs={"source_name": "csv_sales"},
        )

        merge_customers = PythonOperator(
            task_id="merge_customers_raw_to_staging",
            python_callable=merge_raw_to_staging,
            op_kwargs={"source_name": "api_customers"},
        )

        merge_orders = PythonOperator(
            task_id="merge_orders_raw_to_staging",
            python_callable=merge_raw_to_staging,
            op_kwargs={"source_name": "cloudsql_orders"},
        )

    # ----------------------------------------------------
    # Transform: build aggregated views
    # ----------------------------------------------------
    build_aggregations = PythonOperator(
        task_id="build_aggregated_views",
        python_callable=build_aggregated_views,
    )

    # ----------------------------------------------------
    # Data quality checks
    # ----------------------------------------------------
    dq_checks = DataQualityOperator(
        task_id="run_data_quality_checks",
    )

    # ----------------------------------------------------
    # DAG dependencies
    # ----------------------------------------------------
    start >> load_config >> compute_incremental_window >> extract_group
    extract_group >> load_group >> build_aggregations >> dq_checks >> end
