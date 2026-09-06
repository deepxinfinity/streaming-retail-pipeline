"""postgres -> bronze.pg_* daily snapshots. drops today's snapshot_date first so re-runs are safe."""

import argparse
import os
from datetime import date

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

TABLES = ["products", "stores", "customers", "price_list", "promotions", "promo_products"]

# full snapshot every day


def jdbc_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "retail")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=date.fromisoformat, default=date.today())
    snapshot_date = ap.parse_args().date

    spark = SparkSession.builder.appName("extract_dims").getOrCreate()
    for table in TABLES:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url())
            .option("dbtable", table)
            .option("user", os.getenv("POSTGRES_USER", "postgres"))
            .option("password", os.getenv("POSTGRES_PASSWORD", "postgres"))
            .option("driver", "org.postgresql.Driver")
            .load()
            .withColumn("snapshot_date", F.lit(snapshot_date))
        )
        target = f"lake.bronze.pg_{table}"
        if not spark.catalog.tableExists(target):
            df.writeTo(target).using("iceberg").tableProperty("format-version", "2").create()
        else:
            spark.sql(f"DELETE FROM {target} WHERE snapshot_date = date'{snapshot_date}'")
            df.writeTo(target).append()
        print(f"{target}: snapshot {snapshot_date} = {df.count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
