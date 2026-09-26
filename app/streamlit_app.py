"""streamlit ui: what-if price slider + the last optimizer run.

reads the champion model from mlflow, features and recs from gold.
"""

import sys
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ml.common import CHAMPION_ALIAS, MODEL_NAME, mlflow_uri, predict_price_grid  # noqa: E402
from ml.pricing_rules import psych_ending  # noqa: E402

load_dotenv()

st.set_page_config(page_title="Price Intelligence", layout="wide")


@st.cache_resource
def get_model():
    mlflow.set_tracking_uri(mlflow_uri())
    return mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}")


@st.cache_data(ttl=3600)
def get_features() -> pd.DataFrame:
    from ml.dataset import load_features
    df = load_features()
    return (
        df.sort_values("date_day").groupby(["sku_id", "store_id"], observed=True).tail(1)
    )


@st.cache_data(ttl=3600)
def get_recs() -> pd.DataFrame:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("app_recs").getOrCreate()
    if not spark.catalog.tableExists("lake.gold.ml_price_recommendations"):
        return pd.DataFrame()
    recs = spark.sql("""
        SELECT * FROM lake.gold.ml_price_recommendations
        WHERE run_date = (SELECT max(run_date) FROM lake.gold.ml_price_recommendations)
    """).toPandas()
    return recs


page = st.sidebar.radio("Page", ["What-if", "Recommendations"])

if page == "What-if":
    st.title("What-if price explorer")
    feats = get_features()
    model = get_model()

    c1, c2 = st.columns(2)
    sku = c1.selectbox("SKU", sorted(feats.sku_id.unique()))
    stores = sorted(feats[feats.sku_id == sku].store_id.unique())
    store = c2.selectbox("Store", stores)

    row = feats[(feats.sku_id == sku) & (feats.store_id == store)].iloc[0]
    current = float(row.eff_price)
    unit_cost = float(row.unit_cost)

    price = st.slider(
        "Price", min_value=round(current * 0.7, 2), max_value=round(current * 1.3, 2),
        value=round(current, 2), step=0.01,
    )

    multipliers = np.linspace(0.7, 1.3, 61)
    curve = predict_price_grid(model, row, multipliers, zero_promo=False)
    curve["price"] = current * curve["multiplier"]
    curve["revenue"] = curve["price"] * curve["pred_units"]
    curve["margin"] = (curve["price"] - unit_cost) * curve["pred_units"]

    reco_price = psych_ending(float(curve.loc[curve.margin.idxmax(), "price"]))
    at = curve.iloc[(curve.price - price).abs().argmin()]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Current price", f"${current:.2f}")
    m2.metric("Recommended (argmax margin)", f"${reco_price:.2f}")
    m3.metric("Units @ slider", f"{at.pred_units:.1f}/day")
    m4.metric("Margin @ slider", f"${at.margin:.0f}/day")

    chart = curve.set_index("price")[["pred_units", "revenue", "margin"]]
    st.line_chart(chart)
    st.caption(
        f"Demand model: Poisson LightGBM, monotone-constrained in price. "
        f"Cost ${unit_cost:.2f} * list ${float(row.list_price):.2f} * "
        f"category {row.category_name} * {row.region}/{row.store_format}"
    )

else:
    st.title("Latest price recommendations")
    recs = get_recs()
    if recs.empty:
        st.info("No recommendations yet  -  run `make score` (or the nightly_scoring DAG).")
    else:
        st.caption(f"run_date {recs.run_date.max()} * model v{recs.model_version.iloc[0]} "
                   f"* {len(recs):,} pairs")
        c1, c2 = st.columns(2)
        store_f = c1.multiselect("Store", sorted(recs.store_id.unique()))
        only_moves = c2.checkbox("Only price moves", value=True)
        view = recs
        if store_f:
            view = view[view.store_id.isin(store_f)]
        if only_moves:
            view = view[view.recommended_price != view.current_price]
        view = view.sort_values("expected_margin_delta", ascending=False)
        st.dataframe(view, use_container_width=True, height=500)
        st.download_button(
            "Download CSV", view.to_csv(index=False).encode(),
            file_name=f"price_recs_{recs.run_date.max()}.csv",
        )
