"""iceberg namespaces + bronze/silver tables. gold belongs to dbt.

all CREATE IF NOT EXISTS, safe to re-run.
"""

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

LINES_STRUCT = (
    "array<struct<line_no:int,sku_id:string,qty:int,unit_price:double,promo_id:string>>"
)

DDL = [
    "CREATE NAMESPACE IF NOT EXISTS lake.bronze",
    "CREATE NAMESPACE IF NOT EXISTS lake.silver",
    "CREATE NAMESPACE IF NOT EXISTS lake.gold",

    # ---------------- bronze: payload as-is + kafka lineage; APPEND-ONLY ----
    f"""
    CREATE TABLE IF NOT EXISTS lake.bronze.online_orders (
      event_id string, order_id string, order_ts timestamp, customer_id string,
      channel string, lines {LINES_STRUCT},
      _topic string, source_partition int, _offset bigint, _kafka_ts timestamp, _ingest_ts timestamp)
    USING iceberg
    PARTITIONED BY (days(_ingest_ts))
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS lake.bronze.store_pos (
      event_id string, txn_id string, store_id string, pos_ts timestamp, sent_ts timestamp,
      customer_id string, lines {LINES_STRUCT},
      _topic string, source_partition int, _offset bigint, _kafka_ts timestamp, _ingest_ts timestamp)
    USING iceberg
    PARTITIONED BY (days(_ingest_ts))
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
    """,

    # ---------------- silver: deduped facts, event-time partitioned ---------
    """
    CREATE TABLE IF NOT EXISTS lake.silver.sales_lines (
      sale_line_id string, channel string, txn_id string,
      store_id string, customer_id string, sku_id string, qty int,
      unit_price decimal(9,2), line_amount decimal(11,2), promo_id string,
      event_ts timestamp, arrived_ts timestamp)
    USING iceberg
    PARTITIONED BY (days(event_ts))
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
    """,

    # ---------------- silver dims -------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS lake.silver.dim_products (
      sku_id string, sku_name string, department string, category_id string,
      category_name string, brand string, base_price decimal(9,2), unit_cost decimal(9,2),
      active boolean, attrs_hash string,
      valid_from date, valid_to date, is_current boolean)
    USING iceberg TBLPROPERTIES ('format-version'='2')
    """,
    """
    CREATE TABLE IF NOT EXISTS lake.silver.dim_stores (
      store_id string, store_name string, region string, city string,
      store_format string, opened_date date)
    USING iceberg TBLPROPERTIES ('format-version'='2')
    """,
    """
    CREATE TABLE IF NOT EXISTS lake.silver.dim_customers (
      customer_id string, full_name string, segment string,
      home_store_id string, signup_date date)
    USING iceberg TBLPROPERTIES ('format-version'='2')
    """,
    """
    CREATE TABLE IF NOT EXISTS lake.silver.price_history (
      sku_id string, scope string, price decimal(9,2),
      valid_from date, valid_to date)
    USING iceberg TBLPROPERTIES ('format-version'='2')
    """,
]


def main() -> None:
    spark = SparkSession.builder.appName("create_tables").getOrCreate()
    for stmt in DDL:
        spark.sql(stmt)
    print("namespaces + bronze/silver tables ready:")
    for ns in ("bronze", "silver", "gold"):
        for row in spark.sql(f"SHOW TABLES IN lake.{ns}").collect():
            print(f"  lake.{ns}.{row.tableName}")
    spark.stop()


if __name__ == "__main__":
    main()
