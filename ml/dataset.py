import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from ml.common import prep_frame

load_dotenv()

CACHE = Path("data/ml_features.parquet")

# the cache does not expire. delete it after a dbt rebuild or you will train on
# yesterday's features and wonder why the numbers moved.


def load_features(use_cache: bool = True) -> pd.DataFrame:
    if use_cache and CACHE.exists():
        return prep_frame(pd.read_parquet(CACHE))

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("ml_dataset").getOrCreate()
    sdf = spark.table("lake.gold.ml_features_price_demand")
    n_sample = int(os.getenv("SAMPLE_STORES", "0"))
    if n_sample:
        stores = [r.store_id for r in
                  sdf.select("store_id").distinct().limit(n_sample).collect()]
        sdf = sdf.where(sdf.store_id.isin(stores))
    df = sdf.toPandas()
    spark.stop()

    CACHE.parent.mkdir(exist_ok=True)
    df.to_parquet(CACHE, index=False)
    print(f"cached {len(df):,} rows -> {CACHE}")
    return prep_frame(df)


if __name__ == "__main__":
    df = load_features(use_cache=False)
    print(df.describe(include="all").T.head(30))
