"""02:00 UTC - dims snapshot, silver scds, dbt build, reconcile, light maintenance.
dbt tests + reconcile sit between silver and gold, so a bad day of data fails
the run instead of quietly landing in the dashboards.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def reconcile() -> None:
    """fail loudly if gold drifted from silver, counts and amounts must match."""
    from pyspark.sql import SparkSession

    REPO = "/opt/airflow/repo"
    SPARK = SparkSession.builder.appName("daily_dims").getOrCreate()

    spark = SPARK
    try:
        silver = spark.sql(
            "SELECT count(*) AS n, coalesce(sum(line_amount),0) AS amt FROM lake.silver.sales_lines"
        ).collect()[0]
        gold = spark.sql(
            "SELECT count(*) AS n, coalesce(sum(line_amount),0) AS amt FROM lake.gold.fct_sales"
        ).collect()[0]
        if silver.n != gold.n or abs(float(silver.amt) - float(gold.amt)) > 0.01:
            raise ValueError(
                f"RECONCILIATION FAILED: silver rows={silver.n} amt={silver.amt} "
                f"vs gold rows={gold.n} amt={gold.amt}"
            )
        print(f"reconciled: {gold.n:,} rows, amount {gold.amt}")
    finally:
        spark.stop()


with DAG(
    dag_id="daily_dims",
    description="Dims snapshot -> silver SCDs -> dbt gold (tests gate promotion) -> reconcile",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    doc_md=__doc__,
) as dag:
    extract = BashOperator(
        task_id="extract_dims",
        bash_command=f"cd {REPO} && python ingestion/extract_dims.py",
    )
    build_dims = BashOperator(
        task_id="build_silver_dims",
        bash_command=f"cd {REPO} && python ingestion/build_silver_dims.py",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {REPO}/dbt/retail_marts && dbt build --profiles-dir .",
    )
    reconcile_task = PythonOperator(task_id="reconcile", python_callable=reconcile)
    light_maintenance = BashOperator(
        task_id="light_maintenance",
        bash_command=f"cd {REPO} && python lakehouse/maintenance.py",
    )

    extract >> build_dims >> dbt_build >> reconcile_task >> light_maintenance
