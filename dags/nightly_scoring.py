"""03:30 UTC - load the champion model, run the optimizer, write recs to gold.

drift report only runs when DRIFT_ENABLED=1 is set in the airflow env.
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

REPO = "/opt/airflow/repo"

with DAG(
    dag_id="nightly_scoring",
    description="Score latest features with the champion model, write gold.ml_price_recommendations",
    schedule="30 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"owner": "lakehouse", "retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    optimize = BashOperator(
        task_id="optimize_prices",
        bash_command=f"cd {REPO} && python -m ml.optimize",
    )

    if os.getenv("DRIFT_ENABLED") == "1":
        drift = BashOperator(
            task_id="drift_report",
            bash_command=f"cd {REPO} && python -m ml.drift",
        )
        optimize >> drift
