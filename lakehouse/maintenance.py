"""iceberg housekeeping: compaction, snapshot expiry, orphan cleanup.

30s triggers across two topics pile up small files fast. light run daily, full
run weekly, both scheduled from dags.
"""

import argparse
from datetime import UTC, datetime, timedelta

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

TABLES = [
    "lake.bronze.online_orders",
    "lake.bronze.store_pos",
    "lake.silver.sales_lines",
]

TARGET_FILE_SIZE = 134_217_728  # 128 MB
RETAIN_DAYS = 7

# TODO gold tables aren't in here yet, dbt rebuilds them as full tables so it
# hasn't mattered. will need it maybe once agg_sku_store_day goes incremental


def cutoff_ts() -> str:
    """iceberg's CALL parser only takes literals, so build the timestamp here."""
    return (datetime.now(UTC) - timedelta(days=RETAIN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")


def compact(spark: SparkSession, table: str) -> None:
    res = spark.sql(f"""
        CALL lake.system.rewrite_data_files(
          table => '{table}',
          options => map('target-file-size-bytes', '{TARGET_FILE_SIZE}',
                         'min-input-files', '5'))
    """).collect()[0]
    print(f"{table}: rewrote {res.rewritten_data_files_count} files "
          f"-> {res.added_data_files_count}")


def expire_snapshots(spark: SparkSession, table: str) -> None:
    spark.sql(f"""
        CALL lake.system.expire_snapshots(
          table => '{table}',
          older_than => TIMESTAMP '{cutoff_ts()}',
          retain_last => 5)
    """)
    print(f"{table}: snapshots expired (>{RETAIN_DAYS}d, kept last 5)")


def remove_orphans(spark: SparkSession, table: str) -> None:
    res = spark.sql(f"""
        CALL lake.system.remove_orphan_files(
          table => '{table}',
          older_than => TIMESTAMP '{cutoff_ts()}')
    """).collect()
    print(f"{table}: {len(res)} orphan files removed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true",
                    help="also expire snapshots and remove orphan files")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("maintenance").getOrCreate()
    for table in TABLES:
        if not spark.catalog.tableExists(table):
            continue
        compact(spark, table)
        if args.full:
            expire_snapshots(spark, table)
            remove_orphans(spark, table)
    spark.stop()


if __name__ == "__main__":
    main()
