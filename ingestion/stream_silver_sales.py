"""bronze -> silver: explode lines, normalize the two channels, merge on sale_line_id.

reads from bronze (append-only, so it works as a streaming source) and merges
inside foreachBatch.

using merge instead of dropDuplicates+watermark puts the dedupe state in the
table rather than the state store. late pos rows can land whenever they land,
and a kill/restart just replays an idempotent merge. should write this up
properly as an adr at some point.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

load_dotenv()

CHK = os.getenv("CHECKPOINT_DIR", "chk")
TRIGGER = os.getenv("SILVER_TRIGGER", "60 seconds")

MERGE_SQL = """
MERGE INTO lake.silver.sales_lines t
USING silver_batch s
ON t.sale_line_id = s.sale_line_id
WHEN NOT MATCHED THEN INSERT *
"""


def normalize_online(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("event_id"), F.col("order_id").alias("txn_id"),
            F.lit("online").alias("channel"), F.lit("S-ONLINE").alias("store_id"),
            F.col("customer_id"), F.col("order_ts").alias("event_ts"),
            F.col("_ingest_ts").alias("arrived_ts"), F.explode("lines").alias("l"),
        )
    )


def normalize_pos(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("event_id"), F.col("txn_id"),
            F.lit("store").alias("channel"), F.col("store_id"),
            F.col("customer_id"), F.col("pos_ts").alias("event_ts"),
            F.col("_ingest_ts").alias("arrived_ts"), F.explode("lines").alias("l"),
        )
    )


def to_sales_lines(df: DataFrame) -> DataFrame:
    return df.select(
        F.sha2(F.concat_ws(":", "event_id", "l.line_no"), 256).alias("sale_line_id"),
        "channel", "txn_id", "store_id", "customer_id",
        F.col("l.sku_id").alias("sku_id"),
        F.col("l.qty").alias("qty"),
        F.col("l.unit_price").cast("decimal(9,2)").alias("unit_price"),
        (F.col("l.qty") * F.col("l.unit_price")).cast("decimal(11,2)").alias("line_amount"),
        F.col("l.promo_id").alias("promo_id"),
        "event_ts", "arrived_ts",
    ).where(F.col("sku_id").isNotNull() & (F.col("qty") > 0) & F.col("event_ts").isNotNull())


def merge_batch(batch_df: DataFrame, batch_id: int) -> None:
    # TODO isEmpty() seems to kick off a job every micro-batch even when there's
    # nothing to do. probably wasteful at a 60s trigger, look at this later
    if batch_df.isEmpty():
        return
    # dedupe within the batch first - duplicate flushes usually land in the same
    # micro-batch. across batches it's the merge's problem
    deduped = batch_df.dropDuplicates(["sale_line_id"])
    deduped.createOrReplaceTempView("silver_batch")
    deduped.sparkSession.sql(MERGE_SQL)


def main() -> None:
    spark = SparkSession.builder.appName("stream_silver_sales").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    online = spark.readStream.format("iceberg").load("lake.bronze.online_orders")
    pos = spark.readStream.format("iceberg").load("lake.bronze.store_pos")
    # both bronze tables go through one query, so they share a checkpoint. if
    # one source blows up the whole thing restarts. two queries would be safer
    # but this was easier to get working first
    unified = to_sales_lines(normalize_online(online)).unionByName(
        to_sales_lines(normalize_pos(pos))
    )

    q = (
        unified.writeStream
        .foreachBatch(merge_batch)
        .trigger(processingTime=TRIGGER)
        .option("checkpointLocation", f"s3a://lake/{CHK}/silver_sales")
        .start()
    )
    print(f"silver MERGE stream running (trigger={TRIGGER})")
    q.awaitTermination()


if __name__ == "__main__":
    main()
