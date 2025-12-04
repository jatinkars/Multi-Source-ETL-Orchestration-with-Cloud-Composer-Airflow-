import yaml
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import bigquery
from airflow.models import Variable


class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        dq_config_path = Variable.get("DQ_CONFIG_PATH", "config/dq_checks.yaml")

        with open(dq_config_path, "r") as f:
            config = yaml.safe_load(f)

        ti = context["ti"]
        pipeline_config = ti.xcom_pull(task_ids="load_config", key="pipeline_config")
        project_id = pipeline_config["project_id"]
        staging_dataset = pipeline_config["bigquery"]["staging_dataset"]

        client = bigquery.Client(project=project_id)

        for check in config["checks"]:
            name = check["name"]
            query = check["query"].format(
                project_id=project_id,
                staging_dataset=staging_dataset,
            )
            condition = check["condition"]

            self.log.info("Running DQ check '%s': %s", name, query)
            result = client.query(query).result()
            row = list(result)[0]
            # assumes single numeric field
            metric_value = list(row.values())[0]

            self.log.info("Result for '%s': %s", name, metric_value)
            expression = condition.replace("cnt", str(metric_value))
            if not eval(expression):
                raise ValueError(f"Data quality check failed: {name} ({expression})")
