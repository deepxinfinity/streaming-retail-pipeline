"""daily postgres snapshots (bronze.pg_*) -> the dimension tables in silver.

three tables, and their way according to the usecase.

products keeps its history. when one of the tracked columns changes - cost,
price, name, active - the current row is closed off with valid_to = yesterday
and a new row is inserted beside it. one sku ends up with several rows, each
valid for a date range. that is what lets a sale from january be costed with
january's unit_cost instead of today's. the warehouse name for this is a type 2
slowly changing dimension.

stores and customers are just overwritten in place.(scd type 1.)

price_history postgres only records when a price started, never when it
ended. LEAD() takes the end from the next row's start date.
"""

from __future__ import annotations

import argparse
from datetime import date

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

TRACKED = ["sku_name", "department", "category_id", "category_name",
           "brand", "base_price", "unit_cost", "active"]


def latest_snapshot(spark: SparkSession, table: str, snapshot_date: date):
    df = spark.table(table).where(f"snapshot_date <= date'{snapshot_date}'")
    max_snap = df.agg({"snapshot_date": "max"}).collect()[0][0]
    if max_snap is None:
        raise SystemExit(f"{table} has no snapshot <= {snapshot_date}  -  run extract_dims first")
    return df.where(f"snapshot_date = date'{max_snap}'")


def build_products_scd2(spark: SparkSession, snapshot_date: date) -> None:
    hash_expr = f"sha2(concat_ws('||', {', '.join(TRACKED)}), 256)"
    # on the very first load every product gets backdated, because the facts are
    # older than the day we first looked at postgres. dating them from today
    # would leave every backfilled sale with nothing to join to. after that,
    # snapshot_date is right - a new row then really is a change seen that day.
    first_load = spark.sql("SELECT count(*) c FROM lake.silver.dim_products").collect()[0].c == 0
    valid_from = "date'1900-01-01'" if first_load else f"date'{snapshot_date}'"
    latest_snapshot(spark, "lake.bronze.pg_products", snapshot_date) \
        .createOrReplaceTempView("snap_products")
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW staged_products AS
        SELECT sku_id, sku_name, department, category_id, category_name, brand,
               CAST(base_price AS decimal(9,2)) AS base_price,
               CAST(unit_cost  AS decimal(9,2)) AS unit_cost,
               active, {hash_expr} AS attrs_hash
        FROM snap_products
    """)
    # 1) close current rows whose tracked attributes changed
    spark.sql(f"""
        MERGE INTO lake.silver.dim_products t
        USING staged_products s
        ON t.sku_id = s.sku_id AND t.is_current = true
        WHEN MATCHED AND t.attrs_hash <> s.attrs_hash THEN
          UPDATE SET t.valid_to = date_sub(date'{snapshot_date}', 1), t.is_current = false
    """)
    # 2) insert a fresh current version for new SKUs and just-closed ones
    spark.sql(f"""
        INSERT INTO lake.silver.dim_products
        SELECT s.*, {valid_from} AS valid_from, CAST(NULL AS date) AS valid_to,
               true AS is_current
        FROM staged_products s
        LEFT ANTI JOIN (SELECT sku_id FROM lake.silver.dim_products WHERE is_current) c
          ON s.sku_id = c.sku_id
    """)
    n = spark.table("lake.silver.dim_products").where("is_current").count()
    print(f"dim_products (SCD2): {n:,} current rows")


def build_scd1(spark: SparkSession, snapshot_date: date) -> None:
    latest_snapshot(spark, "lake.bronze.pg_stores", snapshot_date) \
        .selectExpr("store_id", "store_name", "region", "city", "store_format",
                    "CAST(opened_date AS date) AS opened_date") \
        .createOrReplaceTempView("snap_stores")
    spark.sql("""
        MERGE INTO lake.silver.dim_stores t USING snap_stores s
        ON t.store_id = s.store_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    latest_snapshot(spark, "lake.bronze.pg_customers", snapshot_date) \
        .selectExpr("customer_id", "full_name", "segment", "home_store_id",
                    "CAST(signup_date AS date) AS signup_date") \
        .createOrReplaceTempView("snap_customers")
    spark.sql("""
        MERGE INTO lake.silver.dim_customers t USING snap_customers s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("dim_stores / dim_customers (SCD1): upserted")


def build_price_history(spark: SparkSession, snapshot_date: date) -> None:
    latest_snapshot(spark, "lake.bronze.pg_price_list", snapshot_date) \
        .createOrReplaceTempView("snap_price_list")
    spark.sql("""
        INSERT OVERWRITE lake.silver.price_history
        SELECT sku_id, scope, CAST(price AS decimal(9,2)) AS price,
               CAST(effective_from AS date) AS valid_from,
               date_sub(LEAD(CAST(effective_from AS date))
                 OVER (PARTITION BY sku_id, scope ORDER BY effective_from), 1) AS valid_to
        FROM snap_price_list
    """)
    n = spark.table("lake.silver.price_history").count()
    print(f"price_history: {n:,} windows")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=date.fromisoformat, default=date.today())
    snapshot_date = ap.parse_args().date

    spark = SparkSession.builder.appName("build_silver_dims").getOrCreate()
    build_products_scd2(spark, snapshot_date)
    build_scd1(spark, snapshot_date)
    build_price_history(spark, snapshot_date)
    spark.stop()


if __name__ == "__main__":
    main()
