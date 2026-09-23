"""shared ml config: feature list, lgb params, metrics.

keep FEATURES in this order. MONOTONE_CONSTRAINTS below is positional, so
reordering the list silently applies the constraint to the wrong column.
"""

import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

TARGET = "units"

NUMERIC_FEATURES = [
    "rel_price",          # has to stay first, see MONOTONE_CONSTRAINTS
    "discount_depth",
    "promo_flag",
    "units_lag_7",
    "units_lag_14",
    "units_ma_28",
    "base_price",
    "dow",
    "week_of_year",
    "month",
    "is_weekend",
    "is_holiday",
]
CATEGORICAL_FEATURES = ["category_name", "department", "store_format", "region"]
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

# -1 on rel_price means the model can never predict more units at a higher
# price. without it a tree happily learns that from noise and the optimizer
# then recommends raising prices to sell more, which is nonsense.
MONOTONE_CONSTRAINTS = [-1] + [0] * (len(FEATURES) - 1)

LGB_PARAMS = {
    "objective": "poisson",       # units are counts, squared error assumes they are not
    "learning_rate": 0.05,
    "num_leaves": 127,
    "min_data_in_leaf": 50,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 1,
    "verbosity": -1,
    "monotone_constraints": MONOTONE_CONSTRAINTS,
}

MODEL_NAME = "price_demand"
CHAMPION_ALIAS = "champion"


def mlflow_uri() -> str:
    return os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")


def wmape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = np.abs(y_true).sum()
    return float(np.abs(y_true - y_pred).sum() / denom) if denom else float("nan")


def bias(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = y_true.sum()
    return float((y_pred - y_true).sum() / denom) if denom else float("nan")


def prep_frame(df: pd.DataFrame) -> pd.DataFrame:
    """dtypes lightgbm wants. leaves sku_id/store_id/date_day alone, they get
    used for slicing later."""
    df = df.copy()
    for c in CATEGORICAL_FEATURES:
        df[c] = df[c].astype("category")
    for c in NUMERIC_FEATURES + [TARGET]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["date_day"] = pd.to_datetime(df["date_day"])
    return df


def predict_price_grid(model, row: pd.Series, multipliers: np.ndarray,
                       zero_promo: bool = True) -> pd.DataFrame:
    """copy one row across a range of prices and predict units for each.

    zero_promo drops promo_flag and discount_depth, which you want when you are
    measuring the price response on its own - otherwise a discounted row gives
    you price and promo mixed together. the optimizer leaves them in.
    """
    grid = pd.DataFrame({c: [row[c]] * len(multipliers) for c in FEATURES})
    for c in NUMERIC_FEATURES:
        grid[c] = pd.to_numeric(grid[c])
    for c in CATEGORICAL_FEATURES:
        grid[c] = grid[c].astype("category")
    grid["rel_price"] = float(row["rel_price"]) * multipliers
    if zero_promo:
        grid["promo_flag"] = 0
        grid["discount_depth"] = 0.0
    pred = np.clip(model.predict(grid[FEATURES]), 1e-6, None)
    return pd.DataFrame({
        "multiplier": multipliers,
        "rel_price": grid["rel_price"].to_numpy(),
        "pred_units": pred,
    })
