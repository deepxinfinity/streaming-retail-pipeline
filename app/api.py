"""optional fastapi wrapper around the what-if calc. POST /whatif"""

import sys
from functools import lru_cache
from pathlib import Path

import mlflow
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ml.common import CHAMPION_ALIAS, MODEL_NAME, mlflow_uri, predict_price_grid  # noqa: E402

app = FastAPI(title="price-intelligence")


class WhatIf(BaseModel):
    sku_id: str
    store_id: str
    price: float


@lru_cache(maxsize=1)
def _model():
    mlflow.set_tracking_uri(mlflow_uri())
    return mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")


@lru_cache(maxsize=1)
def _features():
    from ml.dataset import load_features
    df = load_features()
    return df.sort_values("date_day").groupby(["sku_id", "store_id"], observed=True).tail(1)


@app.post("/whatif")
def whatif(q: WhatIf) -> dict:
    feats = _features()
    match = feats[(feats.sku_id == q.sku_id) & (feats.store_id == q.store_id)]
    if match.empty:
        raise HTTPException(404, f"no features for ({q.sku_id}, {q.store_id})")
    row = match.iloc[0]
    current = float(row.eff_price)
    if current <= 0:
        raise HTTPException(422, "current price unknown")

    import numpy as np
    curve = predict_price_grid(_model(), row, np.array([q.price / current]), zero_promo=False)
    units = float(curve.pred_units.iloc[0])
    return {
        "sku_id": q.sku_id,
        "store_id": q.store_id,
        "price": q.price,
        "current_price": current,
        "pred_units_per_day": units,
        "pred_revenue_per_day": units * q.price,
        "pred_margin_per_day": units * (q.price - float(row.unit_cost)),
    }
