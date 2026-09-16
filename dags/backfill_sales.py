"""manually triggered replay of [start, end) through kafka.

safe to re-run a window, the silver merge absorbs the duplicates.
"""

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

REPO = "/opt/airflow/repo"

with DAG(
    dag_id="backfill_sales",
    description="Manual: replay a date window through Kafka (idempotent via silver MERGE)",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    params={"start": "2026-01-01", "end": "2026-01-08"},
    default_args={"owner": "lakehouse"},
) as dag:
    BashOperator(
        task_id="replay_window",
        bash_command=(
            f"cd {REPO} && python -m sim.backfill "
            "--start {{ params.start }} --end {{ params.end }}"
        ),
    )
