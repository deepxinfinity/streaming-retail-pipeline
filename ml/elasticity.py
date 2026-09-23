"""does the model recover the elasticities the simulator planted?

the simulator drew a price elasticity per sku and wrote it to sim/params.json,
which never gets ingested. so: sweep each product across a price range, fit a
straight line through log(price) vs log(units) - the slope of that line IS the
elasticity - and compare to what the simulator actually used.

you almost never get to do this. normally there is no answer key.

writes reports/elasticity_recovery.png
"""

import json
from collections import defaultdict
from pathlib import Path

import matplotlib
import mlflow
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from ml.common import (  # noqa: E402
    CHAMPION_ALIAS,
    MODEL_NAME,
    mlflow_uri,
    predict_price_grid,
)
from ml.dataset import load_features  # noqa: E402

PARAMS_PATH = Path("sim/params.json")
OUT = Path("reports/elasticity_recovery.png")
GRID = np.round(np.arange(0.80, 1.2001, 0.05), 2)
PAIRS_PER_CATEGORY = 40


def load_champion():
    mlflow.set_tracking_uri(mlflow_uri())
    return mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")


def recovered_elasticities(model, df: pd.DataFrame) -> pd.Series:
    """average implied elasticity per category.

    one (sku, store) pair per row, capped at PAIRS_PER_CATEGORY because this
    predicts a whole grid per row and gets slow otherwise.
    """
    latest = (
        df.sort_values("date_day")
        .groupby(["sku_id", "store_id"], observed=True)
        .tail(1)
        .groupby("category_name", observed=True)
        .head(PAIRS_PER_CATEGORY)
        .copy()
    )
    slopes: dict[str, list[float]] = defaultdict(list)
    for _, row in latest.iterrows():
        curve = predict_price_grid(model, row, GRID, zero_promo=True)
        slope = np.polyfit(np.log(curve["rel_price"]), np.log(curve["pred_units"]), 1)[0]
        slopes[str(row["category_name"])].append(float(slope))
    return pd.Series({k: float(np.mean(v)) for k, v in slopes.items()}, name="recovered")


def true_elasticities() -> pd.Series:
    if not PARAMS_PATH.exists():
        raise SystemExit("sim/params.json missing, run `make seed` - no answer key without it")
    params = json.loads(PARAMS_PATH.read_text())
    by_cat: dict[str, list[float]] = defaultdict(list)
    for sku in params["skus"].values():
        by_cat[sku["category_name"]].append(sku["elasticity"])
    return pd.Series({k: float(np.mean(v)) for k, v in by_cat.items()}, name="true")


def main():
    model = load_champion()
    df = load_features()
    both = pd.concat([true_elasticities(), recovered_elasticities(model, df)], axis=1).dropna()
    mae = float((both["true"] - both["recovered"]).abs().mean())

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.scatter(both["true"], both["recovered"], s=45, alpha=0.75)
    lims = [both.min().min() - 0.2, both.max().max() + 0.2]
    ax.plot(lims, lims, "k--", lw=1, label="perfect recovery")
    ax.set_xlabel("true elasticity (planted in simulator)")
    ax.set_ylabel("recovered elasticity (model log-log slope)")
    ax.set_title(f"elasticity recovery by category (MAE {mae:.2f})")
    ax.legend()
    OUT.parent.mkdir(exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUT, dpi=150)

    mlflow.set_experiment("price_demand_elasticity")
    with mlflow.start_run(run_name="recovery"):
        mlflow.log_metric("elasticity_mae", mae)
        mlflow.log_artifact(str(OUT))
    print(f"MAE {mae:.3f} over {len(both)} categories -> {OUT}")
    print(both.round(2).to_string())


if __name__ == "__main__":
    main()
