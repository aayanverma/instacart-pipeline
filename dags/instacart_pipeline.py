"""
instacart_pipeline.py

Orchestrates the full Instacart ELT pipeline end-to-end:
  1. ingest      -- Python script loads raw CSVs into Snowflake raw schema
  2. dbt_run     -- dbt builds all staging and mart models
  3. dbt_test    -- dbt runs all 69 data quality tests
  4. dbt_docs    -- dbt generates documentation and lineage catalog

DAG design decisions worth being able to explain:
- BashOperator throughout: dbt is a CLI tool, BashOperator is the standard,
  defensible pattern for shelling out to it. No custom Python operators needed
  since dbt itself handles the Snowflake connection and compilation.
- Linear task dependencies (no fan-out): each step genuinely depends on the
  previous one completing successfully -- you can't run dbt models before raw
  data exists, can't test models before they're built, can't generate docs
  before models exist. The dependency chain is real, not cosmetic.
- retries=2 on ingest and dbt_run: the two steps most likely to fail on
  transient issues (network to Snowflake, large-file upload timeouts).
  dbt_test intentionally has retries=0 -- a test failure is a data quality
  signal that should surface immediately, not be retried away.
- on_failure_callback not implemented here (would add alerting in production,
  e.g. Slack/PagerDuty notification on DAG failure).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = '/opt/airflow/dbt_instacart'
INGESTION_DIR = '/opt/airflow/ingestion'
DATA_DIR = '/opt/airflow/data/raw_sample'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='instacart_pipeline',
    default_args=default_args,
    description='Full Instacart ELT: ingest -> dbt run -> dbt test -> dbt docs',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['instacart', 'dbt', 'snowflake'],
) as dag:

    ingest = BashOperator(
        task_id='ingest',
        bash_command=f'python3 {INGESTION_DIR}/load_to_snowflake.py --source {DATA_DIR}',
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && /home/airflow/.local/bin/dbt run --profiles-dir /home/airflow/.dbt',
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && /home/airflow/.local/bin/dbt test --profiles-dir /home/airflow/.dbt',
        retries=0,
    )

    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'cd {DBT_DIR} && /home/airflow/.local/bin/dbt docs generate --profiles-dir /home/airflow/.dbt',
        retries=1,
    )

    # Linear dependency chain -- each step genuinely requires the previous one
    ingest >> dbt_run >> dbt_test >> dbt_docs