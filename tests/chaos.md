# Failure tests

"Exactly-once" should be something I checked, not something I assumed. These are the ways I try
to break the pipeline.

## What's checked so far

Draining a 6-month backfill, 2,241,188 Kafka messages:

| Check | Result |
|---|---|
| bronze rows == Kafka messages | 2,241,188 == 2,241,188 |
| duplicate store uploads reaching bronze | 18,569 (1.05%, on purpose) |
| duplicates surviving into silver | **0** |
| `count(*)` == `count(distinct sale_line_id)` | yes, 15,315,700 |
| gold reconciles to silver | exactly |

That's task 4, and it's the one the whole design is for. Numbers below.

## Task list

Task 4 is done. The rest are still to do, listed here so it's clear which claims are measured
and which aren't.

| # | Task | How | What should happen |
|---|------|-----|--------------------|
| 1 | Kill the silver job | `kill -9` the `stream_silver_sales.py` PID mid-batch, restart it | Picks up from its checkpoint; `count(*) == count(distinct sale_line_id)` still holds |
| 2 | Kill the bronze job | `kill -9` `stream_bronze.py`, restart | Re-reads from its checkpoint; bronze rows == topic messages |
| 3 | Restart Kafka | `docker restart rl-kafka` while the producers run | Producers reconnect, no messages lost |
| 4 | Send duplicates | The store producer re-sends ~1% of uploads, same event_ids | Bronze keeps them, silver doesn't |
| 5 | Send broken messages | `kafka-console-producer` a few junk lines into `store_pos` | They go to `sales_dlq`, not bronze, and the jobs keep running |
| 6 | Late data | `POS_DELAY_SCALE=1.0`, wait for an upload delayed over an hour | Rows land in silver under the day the sale happened, not the day it arrived |

## Task 4 result, 6-month backfill

The store producer re-sends about 1% of its uploads with the same `event_id`s, copying what a
till does when an upload half-fails and it retries. After draining 2,241,188 messages:

| Table | Rows | Distinct ids | Duplicates |
|---|---|---|---|
| `bronze.store_pos` | 1,774,090 | 1,755,521 (`event_id`) | **18,569** |
| `bronze.online_orders` | 467,098 | 467,098 (`event_id`) | 0 |
| `silver.sales_lines` | 15,315,700 | 15,315,700 (`sale_line_id`) | **0** |

18,569 duplicates (1.05%, matching `DUP_FLUSH_P = 0.01`) reached bronze and none reached silver.
Bronze keeps them because bronze is whatever we were sent; the MERGE on `sale_line_id` drops them
on the way to silver. Online orders show 0, which is right, because only the store producer
re-sends.

Bronze also matched the Kafka message count exactly: 2,241,188 == 2,241,188.

## Three things that broke by themselves

None of these were planned tests. All three are fixed.

**The catalog wasn't being saved anywhere.** `iceberg-rest` had no `CATALOG_URI` set, so it used
SQLite in memory. When the container was recreated, every table registration went with it, while
all 574 Parquet files sat untouched in MinIO. `SHOW NAMESPACES IN lake` came back empty.

Getting it back was one `register_table` call per table, pointing at each table's newest
`metadata.json`. All 13 tables came back, `silver.sales_lines` included, at the same row count as
before. Nothing was lost.

That's worth knowing: the catalog only stores *where* the current `metadata.json` is, so losing
it costs a re-registration, not the data. Better to find that out on a laptop.

**SQLite couldn't handle the write load.** Putting the catalog in SQLite on a volume fixed the
saving problem and then broke under load. The bronze job runs four streaming queries at once (two
tables, two DLQ branches), all committing every 30 seconds, and SQLite only allows one writer at a
time. The server started returning `SQLITE_BUSY: database is locked`, which Spark reported as
`CommitStateUnknownException: 500`. The catalog now runs on Postgres, which was already there.
See the comment on the `iceberg-rest` service in `docker/compose.yaml`.

**The two jobs keep their checkpoints in different places.** Bronze writes to local disk, silver
to `s3a://lake/chk/`. So clearing the checkpoints takes two steps, and I only did one. Silver
started up again pointing at bronze snapshots that no longer existed:
`Cannot load current offset at snapshot ..., the snapshot was expired or removed`. Worth making
these consistent.

## Reconciliation check

```bash
# after a drain, both numbers must match
SPARK_CONF_DIR=$PWD/docker .venv/bin/python -c "
from pyspark.sql import SparkSession
s = SparkSession.builder.appName('recon').getOrCreate()
s.sql('select count(*), count(distinct sale_line_id) from lake.silver.sales_lines').show()"
```

`scripts/emit_totals.py` adds up what the producers say they sent, but `logs/emit_log.jsonl` keeps
growing across runs, so it only adds up correctly if you clear it before the run you want to
measure. Kafka offsets are the more reliable check.
