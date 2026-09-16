from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

REPO = "/opt/airflow/repo"

with DAG(
    dag_id="weekly_maintenance",
    description="Full Iceberg maintenance: compact, expire snapshots, remove orphans",
    schedule="0 3 * * 0",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "lakehouse", "retries": 1, "retry_delay": timedelta(minutes=10)},
) as dag:
    BashOperator(
        task_id="full_maintenance",
        bash_command=f"cd {REPO} && python lakehouse/maintenance.py --full",
    )
