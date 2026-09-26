"""re-point the bigquery external tables at iceberg's newest metadata.json.

biglake external tables reference one specific metadata file and dbt writes a
new one every build, so this has to run after dbt. google has been shipping
native iceberg rest support that removes the step - check before extending it.

env: GCP_PROJECT, BQ_DATASET, GCS_BUCKET, BQ_CONNECTION
"""

import os
import re
import sys

from google.cloud import bigquery, storage

TABLES = ["agg_sku_store_day", "fct_sales", "ml_price_recommendations"]
GOLD_PREFIX = "warehouse/gold"


def newest_metadata(client: storage.Client, bucket: str, table: str) -> str:
    prefix = f"{GOLD_PREFIX}/{table}/metadata/"
    blobs = [b.name for b in client.list_blobs(bucket, prefix=prefix)
             if b.name.endswith(".metadata.json")]
    if not blobs:
        raise SystemExit(f"no metadata files under gs://{bucket}/{prefix}")
    # iceberg metadata files are v<N>-... or <N>-...  -  order by the numeric version
    def version(name: str) -> int:
        m = re.search(r"(?:^|/)v?(\d+)-[^/]*\.metadata\.json$", name)
        return int(m.group(1)) if m else -1
    return max(blobs, key=version)


def main():
    project = os.environ["GCP_PROJECT"]
    dataset = os.getenv("BQ_DATASET", "analytics")
    bucket = os.environ["GCS_BUCKET"]
    connection = os.getenv("BQ_CONNECTION", "us-central1.lake_conn")
    tables = sys.argv[1:] or TABLES

    gcs = storage.Client(project=project)
    bq = bigquery.Client(project=project)
    for table in tables:
        meta = newest_metadata(gcs, bucket, table)
        uri = f"gs://{bucket}/{meta}"
        bq.query(f"""
            CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.{table}`
            WITH CONNECTION `{connection}`
            OPTIONS (format = 'ICEBERG', uris = ['{uri}'])
        """).result()
        print(f"{dataset}.{table} -> {uri}")


if __name__ == "__main__":
    main()
