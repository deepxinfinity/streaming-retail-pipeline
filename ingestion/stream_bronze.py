"""kafka -> bronze iceberg, append only, 30s triggers.

one query per topic, separate checkpoints. rows where from_json returns null
get written to the sales_dlq topic instead of the table so a malformed payload
doesn't vanish silently.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CHK = os.getenv("CHECKPOINT_DIR", "chk")
TRIGGER = os.getenv("BRONZE_TRIGGER", "30 seconds")
DLQ_TOPIC = os.getenv("TOPIC_DLQ", "sales_dlq")

LINE = StructType([
    StructField("line_no", IntegerType()),
    StructField("sku_id", StringType()),
    StructField("qty", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("promo_id", StringType()),
])

ONLINE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("order_ts", TimestampType()),
    StructField("customer_id", StringType()),
    StructField("channel", StringType()),
    StructField("lines", ArrayType(LINE)),
])

POS_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("txn_id", StringType()),
    StructField("store_id", StringType()),
    StructField("pos_ts", TimestampType()),
    StructField("sent_ts", TimestampType()),
    StructField("customer_id", StringType()),
    StructField("lines", ArrayType(LINE)),
])

# a row is malformed if these can't be parsed  -  it goes to the DLQ
CRITICAL = {"online_orders": ["event_id", "order_ts"], "store_pos": ["event_id", "pos_ts"]}


def read_topic(spark: SparkSession, topic: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 200_000)
        .load()
    )


def start_bronze(spark: SparkSession, topic: str, schema: StructType, table: str):
    parsed = (
        read_topic(spark, topic)
        .select(
            F.col("value").cast("string").alias("_raw"),
            F.from_json(F.col("value").cast("string"), schema).alias("e"),
            F.col("topic").alias("_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("_offset"),
            F.col("timestamp").alias("_kafka_ts"),
        )
    )
    ok = F.lit(True)
    for c in CRITICAL[topic]:
        ok = ok & F.col(f"e.{c}").isNotNull()

    good = (
        parsed.where(ok)
        .select("e.*", "_topic", "source_partition", "_offset", "_kafka_ts")
        .withColumn("_ingest_ts", F.current_timestamp())
        .writeStream.format("iceberg")
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .option("checkpointLocation", f"{CHK}/bronze_{topic}")
        .toTable(table)
    )

    dlq = (
        parsed.where(~ok)
        .select(
            F.to_json(F.struct(
                F.col("_raw").alias("raw"),
                F.col("_topic").alias("source_topic"),
                F.col("source_partition"),
                F.col("_offset").alias("source_offset"),
                F.current_timestamp().cast("string").alias("dlq_ts"),
            )).alias("value")
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("topic", DLQ_TOPIC)
        .trigger(processingTime=TRIGGER)
        .option("checkpointLocation", f"{CHK}/dlq_{topic}")
        .start()
    )
    return good, dlq


def main() -> None:
    spark = SparkSession.builder.appName("stream_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    start_bronze(spark, "online_orders", ONLINE_SCHEMA, "lake.bronze.online_orders")
    start_bronze(spark, "store_pos", POS_SCHEMA, "lake.bronze.store_pos")
    print(f"bronze streams running (trigger={TRIGGER}, checkpoints under {CHK}/)")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
