"""nightly optimizer: champion model -> one recommendation per (sku, store).

for each pair, try prices from -20% to +20% in 5% steps, ask the model how many
units each would sell, and keep the one with the best margin that still passes
the guardrails in pricing_rules.

optimizing for margin rather than revenue on purpose - the highest-revenue price
is usually the lowest one, which is not a useful recommendation.

re-running the same day replaces that day's rows rather than adding to them.
"""

from __future__ import annotations

from datetime import date

import mlflow
import numpy as np
import pandas as pd
from dotenv import load_dotenv

from ml.common import CHAMPION_ALIAS, MODEL_NAME, mlflow_uri, predict_price_grid
from ml.dataset import load_features
from ml.pricing_rules import above_margin_floor, psych_ending, within_move_cap

load_dotenv()

MULTIPLIERS = np.round(np.arange(0.80, 1.2001, 0.05), 2)
TARGET_TABLE = "lake.gold.ml_price_recommendations"
MAX_PAIRS = int(1e9)  # drop to ~20000 if the nightly run drags


def recommend_row(model, row: pd.Series) -> dict | None:
    current_price = float(row["eff_price"])
    unit_cost = float(row["unit_cost"])
    base_price = float(row["base_price"])
    if current_price <= 0 or unit_cost <= 0:
        return None

    curve = predict_price_grid(model, row, MULTIPLIERS, zero_promo=False)
    # the grid scales rel_price, so the shelf price scales by the same factor
    curve["price"] = [psych_ending(current_price * m) for m in curve["multiplier"]]
    curve = curve.drop_duplicates("price")
    curve["revenue"] = curve["price"] * curve["pred_units"]
    curve["margin"] = (curve["price"] - unit_cost) * curve["pred_units"]

    ok = curve[
        curve["price"].apply(lambda p: within_move_cap(current_price, p))
        & curve["price"].apply(lambda p: above_margin_floor(p, unit_cost))
    ]
    if ok.empty:
        return None
    best = ok.loc[ok["margin"].idxmax()]

    # nearest grid point to today's price, used as the "do nothing" baseline.
    # TODO slightly wrong - psych_ending() can round two multipliers onto the
    # same price so the baseline drifts a cent or two off the real one
    cur = curve.iloc[(curve["price"] - current_price).abs().argmin()]
    return {
        "sku_id": str(row["sku_id"]),
        "store_id": str(row["store_id"]),
        "current_price": round(current_price, 2),
        "recommended_price": float(best["price"]),
        "base_price": round(base_price, 2),
        "expected_units_current": float(cur["pred_units"]),
        "expected_units_reco": float(best["pred_units"]),
        "expected_revenue_delta": float(best["revenue"] - cur["revenue"]),
        "expected_margin_delta": float(best["margin"] - cur["margin"]),
    }


def main():
    mlflow.set_tracking_uri(mlflow_uri())
    model = mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")
    client = mlflow.MlflowClient()
    model_version = client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS).version

    df = load_features()
    latest = (
        df.sort_values("date_day")
        .groupby(["sku_id", "store_id"], observed=True)
        .tail(1)
        .head(MAX_PAIRS)
    )
    print(f"optimizing {len(latest):,} (sku, store) pairs with model v{model_version}")

    recs = [r for _, row in latest.iterrows() if (r := recommend_row(model, row))]
    out = pd.DataFrame(recs)
    out.insert(0, "run_date", date.today())
    out.insert(1, "model_version", str(model_version))
    moved = out[out.recommended_price != out.current_price]
    print(f"{len(out):,} recommendations, {len(moved):,} price moves, "
          f"expected margin delta {out.expected_margin_delta.sum():,.0f}")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("write_recs").getOrCreate()
    sdf = spark.createDataFrame(out)
    if not spark.catalog.tableExists(TARGET_TABLE):
        sdf.writeTo(TARGET_TABLE).using("iceberg").tableProperty("format-version", "2").create()
    else:
        spark.sql(f"DELETE FROM {TARGET_TABLE} WHERE run_date = date'{date.today()}'")
        sdf.writeTo(TARGET_TABLE).append()
    print(f"-> {TARGET_TABLE} (run_date={date.today()})")
    spark.stop()


if __name__ == "__main__":
    main()
