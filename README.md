# Streaming Retail Pipeline

A data pipeline for an imaginary supermarket chain, running entirely on a laptop.

The chain sells two ways. The **website** records a sale the moment you click buy. A **physical
store** does not: it rings up a sale at 2pm, holds onto it, and uploads whenever it next has a
connection, which might be twenty minutes later or six hours. Sometimes an upload half-fails
and the store sends the same batch twice.

So we have two streams of the same kind of event, one punctual and one chaotic, and we need
one table an analyst can trust. That's the whole problem this project is about.

I built it to get hands-on with the parts of a data platform I don't touch day to day: how
streaming jobs recover from crashes, how a table format handles duplicates arriving hours late,
and how much of the "exactly-once" story is really the table doing the work rather than the job.

**Stack:** Python * Postgres * Kafka * Spark Structured Streaming * Iceberg on MinIO * dbt *
Airflow * LightGBM + MLflow + Streamlit

It works end to end on six months of simulated trading: 2.2 million events through Kafka into
Iceberg, 15.3 million sale lines in the cleaned layer with zero double-counting despite 18,569
genuinely duplicated till uploads, a reporting layer that reconciles to the penny, and a demand
model that beats a non-trivial baseline. Numbers below.

## Vocabulary

Three words that appear throughout, if you don't work in retail:

- **SKU** - one specific sellable product. "Stock Keeping Unit". A 500ml bottle of a particular
  brand of milk is one SKU; the 1L bottle is a different SKU. This chain has 1,500 of them.
- **POS** - "Point of Sale", meaning the till in a shop. A POS event is one shopper's basket.
- **Elasticity** - one number per product answering "if I raise the price 1%, what percent of
  unit sales do I lose?" A value of `-1.5` means a 1% price rise costs 1.5% of units. Every
  retailer wants this number and it is notoriously hard to measure.

## Status

| Layer | State |
|---|---|
| Simulator + Kafka producers | working |
| Bronze / silver streaming | working, exactly-once verified |
| Silver dimensions (history-tracking) | working |
| dbt gold layer | 37 models and tests, all green |
| Demand forecasting | working, beats both baselines |
| Airflow DAGs | written and running locally |
| Price optimizer | pipeline complete; estimates need the model work in ADR-001 |
| Elasticity estimation | measured and found wanting - ADR-001. The interesting result. |
| Throughput benchmarks | not measured |
| GCP deploy | later |

## Quickstart

```bash
cp .env.example .env
make venv && source .venv/bin/activate
make up                # postgres, kafka, minio, iceberg-rest, mlflow, airflow
make seed              # invent 1,500 products and 50 stores, load into postgres
make ddl               # create the iceberg tables
make backfill M=6      # replay 6 months of shopping through kafka
make stream            # start the two streaming jobs
make dims && make dbt  # build the dimension tables, then the reporting layer
```

The ML side needs extra dependencies:
`make venv-ml && make backtest && make train && make elasticity && make score && make app`.

Set `POS_DELAY_SCALE=0.05` in `.env` while developing. At the realistic default the simulated
tills hold each basket for a median of 20 real minutes before uploading, and you'll think
nothing is working.

## How the data moves

Three layers, and data only ever flows downhill. Each one has exactly one job, and each is
rebuildable from the one above it.

**Bronze - what we were sent.** The raw event exactly as it arrived, plus where it came from.
Append-only. Duplicates live here. Nothing is cleaned. If the parsing logic turns out to be
wrong in six months, this is what you replay from, so it is never edited.

**Silver - what we believe happened.** Cleaned, deduplicated, one row per sale line. A basket
containing six items becomes six rows. Website and till events are reshaped to look identical.
Organised by *when the sale happened*, not when we heard about it - so "sales on the 14th"
means what you'd expect even for a till that uploaded on the 16th.

**Gold - what the business asks for.** Facts and dimensions, aggregates, a feature table for
the model. Built by dbt. This is what a dashboard reads.

## Numbers

Measured on a 6-month simulated history, M2 MacBook Air, 16GB.

**Volume**

| | |
|---|---|
| Kafka messages | 2,241,188 |
| Bronze rows | 2,241,188 (exact match) |
| Silver sale lines | 15,315,700 |
| Duplicate sale lines in silver | 0, from 18,569 duplicates in bronze |
| Days of history | 207 (179 usable; the model needs 28 days of history per row) |
| Gold fact table | 15,315,700 rows, revenue 62,006,739.56, margin 18,792,297.88 |
| Gold reconciles to silver | exactly, to the cent |

**Forecasting.** Trained on the past, tested on the next 14 days, repeated at six different
cutoff dates so the model never sees the future. Scored with WMAPE, which is total absolute
error divided by total actual units - lower is better, and 0.37 means predictions are off by
about 37% of the volume being predicted.

| Model | WMAPE |
|---|---|
| guess last week's sales | 0.6641 |
| guess the 28-day average | 0.4070 |
| **LightGBM** | **0.3786** |

43% better than the naive guess, and about 7% better than the 28-day average - which is the
comparison that actually matters, since beating "last week" is easy. Results were stable across
all six cutoffs (0.3763 to 0.3803), and the model isn't systematically over- or under-predicting
(bias -0.012). Trained on the full history it scores 0.3729.

**Elasticity estimation.** Mean error 1.40 against true values of 1.0 to 2.2. Why, further down.

**Price optimizer.** 76,496 product-and-store combinations in about 4 minutes, 47,500
recommendations written back as an Iceberg table. It suggests raising 57.7% of prices while
predicting that costs 0.005 units per product - the near-zero price response showing up in the
output. Numbers waiting on ADR-001; the path itself works.

Worth one note: there's a rule saying predicted demand may never *rise* when price rises. It
holds and still isn't enough, because flat demand satisfies it, and flat demand makes every
increase look like free profit. The 10% movement cap is what actually bounds the output.


## Layout

| Path | What |
|---|---|
| `sim/` | the imaginary retailer: product catalogue, demand formula, kafka producers |
| `ingestion/` | kafka to bronze, bronze to silver, daily dimension snapshots |
| `lakehouse/` | table creation, file compaction, snapshot expiry |
| `dbt/retail_marts/` | the reporting layer and its tests |
| `dags/` | airflow schedules |
| `ml/` | backtest, training, elasticity estimation, price optimizer |
| `app/` | streamlit price what-if UI |
| `infra/` | gcp helpers |
| `docs/adr/` | decisions worth explaining |

## The bits I found interesting

**Handling duplicates in the table, not in memory.** The usual advice is to remember every ID
you've seen recently and drop repeats. But "recently" has to be bounded or you run out of
memory, so you pick a window - say six hours - and then a till uploads after seven and you
silently count a sale twice. You end up tuning a memory window against the worst-case
behaviour of a shop's wifi.

Instead, every sale line gets an ID derived from its own contents (a hash of the event ID and
line number). Identical input always produces an identical ID, so a duplicate event produces a
byte-identical ID. Loading then asks "is this ID already in the table?" rather than "have I
seen it lately". The memory is the table, so it has no time limit: a till can upload three
weeks late and it still works, and a job that crashes mid-write can safely redo the same work.

That last property is what "exactly-once" actually means in practice. The job may *process* an
event more than once; the *effect on the table* happens once. You don't get there by processing
carefully. You get there by making the write repeatable.

Measured: 18,569 duplicate till uploads reached bronze on the last run, and zero reached
silver. Details in [tests/chaos.md](tests/chaos.md).

**Costing a sale at the price that was true that day.** A product's cost to the retailer
changes. If your product table only holds today's cost, then profit on a sale from January gets
calculated with June's cost, and every historical margin number is wrong.

So the product table keeps one row per *version* of a product, each valid for a date range, and
sales join on both the product and the date falling inside that range. A January sale picks up
January's cost. This is the fiddliest join in the project and the easiest one to get subtly
wrong - I got it wrong once already, and every sale silently vanished from the output.

**Marking my own homework.** The simulator picks a price elasticity for each product and writes
it to a file that never enters the pipeline. So I can train a model on the sales alone, ask it
what the elasticities were, and check the answer against the real one. In a real job you can't -
nobody knows the true value, which is why a wrong answer there looks exactly like a right one.

The answer was wrong. True values run -1.0 to -2.2; the model implies -0.17 to -0.38.

The cause is worth knowing, because it isn't a bug. The model's most useful input is "how many
units did this sell recently", which is 78% of what it relies on; price is 0.9%. If a price rose
two weeks ago then recent sales are already lower, so recent sales quietly carry the price effect
and the model never needs to learn about price itself. Asking "what if price changed but recent
sales stayed the same" describes something that can't happen, so the answer comes back flat.

**A model that predicts well is not automatically a model you can read cause and effect out of.**
The forecasting numbers above are unaffected - they're measured against held-out data. What this
rules out is the bigger claim, that the same model explains *why* demand moves.

Full argument and the two things that would fix it:
[ADR-001](docs/adr/001-price-elasticity-is-not-identifiable-from-the-forecast-model.md).

## Deliberate at this scale

Choices that are right for 1,500 products on a laptop and would not survive a real catalogue:

- Dimension tables rebuild from a full daily snapshot rather than a change feed. Simple, and
  cheap at this size.
- The reporting layer is fully rebuilt on every dbt run, which keeps it easy to reason about.
  Becomes incremental once history grows.
- Events on the wire are JSON, so you can read them with `kafka-console-consumer` while
  debugging. Avro with a schema registry is the next step.

## Next up

- Fit elasticity properly: a separate model without the recent-sales inputs, with per-product
  fixed effects. Detailed in ADR-001. This unblocks the optimizer's estimates.
- Split the silver job in two so one failing source doesn't restart the other.
- Put both streaming jobs' checkpoints in the same place; they currently differ, which makes
  resetting them a two-step job that's easy to half-do.
- Clear `logs/emit_log.jsonl` per run, or make `scripts/emit_totals.py` filter by run.

## License

MIT
