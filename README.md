# Multi-Source-ETL-Orchestration-with-Cloud-Composer-Airflow-
Cloud Composer, BigQuery, Cloud Storage, Python (+ Dataflow or BQ SQL)

This project demonstrates a **GCP-native, fully orchestrated ETL pipeline** using:

- **Cloud Composer (Airflow)** for orchestration
- **Cloud Storage (GCS)** for landing files
- **BigQuery** for staging, transformations and aggregated views
- **Python** for custom operators and extract/load logic
- (Optional) **Dataflow** for heavy transforms, or you can keep everything in BigQuery SQL

The pipeline pulls data from **three different sources**:

1. **CSV files** in a GCS landing bucket  
2. An external **HTTP API**  
3. A **Cloud SQL** transactional database  

It then:

- Loads data into **BigQuery staging tables**
- Applies **incremental loads** based on `updated_at` timestamps
- Builds **aggregated views** for analytics
- Runs **data quality checks**
- Sends **alerts** when something breaks

---

## Architecture

High-level flow:

1. **Extract**
   - Read daily CSV files from GCS (`gs://<landing-bucket>/sales/YYYY/MM/DD/sales_{{ ds }}.csv`)
   - Call an external REST API (e.g. `/customers`) and land data in a BigQuery staging table
   - Query Cloud SQL for changes since last run (incremental) and land in BigQuery

2. **Transform**
   - Use BigQuery jobs to:
     - Clean and standardise staging data
     - Join sources into a consolidated model
     - Build rolled-up, aggregated tables and views

3. **Data Quality**
   - Row count checks on staging tables
   - Non-null checks on primary keys
   - Custom SQL checks defined in `config/dq_checks.yaml`

4. **Orchestration**
   - **Daily** DAG scheduled in Cloud Composer
   - Task dependencies enforced to avoid partial loads
   - **Incremental window** computed per run and passed via Airflow `XCom`

5. **Alerts**
   - Airflow’s built-in email/Slack alerts for task failures
   - Optional: separate `dq_failure` task when quality checks fail

---

## GCP Resources

You will need:

- **Project**: `YOUR_GCP_PROJECT_ID`
- **Cloud Composer environment** (Airflow 2.x)
- **BigQuery dataset**: `raw`, `staging`, `analytics`
- **GCS bucket**: e.g. `gs://your-etl-landing-bucket`
- **Cloud SQL instance** (MySQL or Postgres) for transactional data
- (Optional) **Pub/Sub** and **Dataflow** if you extend beyond BigQuery SQL

---

## Configuration
Example `pipeline_config.yaml`:

```yaml
project_id: my-gcp-project-id
location: us-central1

gcs:
  landing_bucket: your-etl-landing-bucket

bigquery:
  raw_dataset: raw
  staging_dataset: staging
  analytics_dataset: analytics

sources:
  csv_sales:
    gcs_prefix: sales
    file_pattern: "sales_{{ ds }}.csv"
    target_table: raw.sales

  api_customers:
    endpoint: "/customers"
    target_table: raw.customers
    incremental_column: updated_at

  cloudsql_orders:
    cloudsql_conn_id: cloudsql_default
    query_template: |
      SELECT *
      FROM orders
      WHERE updated_at > '{{ last_loaded_ts }}'
    target_table: raw.orders
    incremental_column: updated_at
