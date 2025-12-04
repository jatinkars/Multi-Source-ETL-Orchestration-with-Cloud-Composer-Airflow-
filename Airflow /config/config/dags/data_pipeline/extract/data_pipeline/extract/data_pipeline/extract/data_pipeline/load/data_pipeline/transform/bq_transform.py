from google.cloud import bigquery


def build_aggregated_views(**context):
    """Build analytics tables (e.g., daily revenue per customer)."""
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="pipeline_config")

    project_id = config["project_id"]
    staging_dataset = config["bigquery"]["staging_dataset"]
    analytics_dataset = config["bigquery"]["analytics_dataset"]

    client = bigquery.Client(project=project_id)

    sql = f"""
    CREATE OR REPLACE TABLE `{project_id}.{analytics_dataset}.daily_customer_revenue` AS
    SELECT
      o.order_date,
      c.customer_id,
      c.customer_name,
      SUM(o.order_amount) AS total_revenue
    FROM `{project_id}.{staging_dataset}.orders` o
    JOIN `{project_id}.{staging_dataset}.customers` c
      ON o.customer_id = c.customer_id
    GROUP BY 1,2,3
    """

    job = client.query(sql)
    job.result()
