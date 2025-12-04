from google.cloud import bigquery


def merge_raw_to_staging(source_name: str, **context):
    """Generic MERGE from raw.<table> into staging.<table> as incremental load."""
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="pipeline_config")
    project_id = config["project_id"]

    raw_dataset = config["bigquery"]["raw_dataset"]
    staging_dataset = config["bigquery"]["staging_dataset"]
    source_cfg = config["sources"][source_name]

    incremental_col = source_cfg["incremental_column"]
    raw_table = source_cfg["target_table"].split(".")[-1]     # e.g. 'sales'
    staging_table = raw_table

    client = bigquery.Client(project=project_id)

    sql = f"""
    MERGE `{project_id}.{staging_dataset}.{staging_table}` T
    USING `{project_id}.{raw_dataset}.{raw_table}` S
    ON T.id = S.id
    WHEN MATCHED AND S.{incremental_col} > T.{incremental_col} THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT ROW
    """

    job = client.query(sql)
    job.result()
