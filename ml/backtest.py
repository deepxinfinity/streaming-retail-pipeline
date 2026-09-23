"""rolling-origin backtest: 6 folds, cutoffs 2 weeks apart, 14 day horizon.

each fold trains on everything up to its cutoff and predicts the next 14 days,
so the model never sees the future. compared against two baselines - last
week's units, and a 28 day average - because "wmape 0.31" means nothing on its
own. every fold and model goes to mlflow.
"""

from datetime import timedelta

import lightgbm as lgb
import mlflow
import numpy as np
import pandas as pd

from ml.common import FEATURES, LGB_PARAMS, TARGET, bias, mlflow_uri, wmape
from ml.dataset import load_features

N_FOLDS = 6
HORIZON_DAYS = 14
STEP_DAYS = 14


def fold_cutoffs(df: pd.DataFrame) -> list[pd.Timestamp]:
    last = df.date_day.max() - timedelta(days=HORIZON_DAYS)
    return [last - timedelta(days=STEP_DAYS * k) for k in range(N_FOLDS)][::-1]


def eval_fold(df: pd.DataFrame, cutoff: pd.Timestamp) -> dict[str, dict[str, float]]:
    train = df[df.date_day <= cutoff]
    test = df[(df.date_day > cutoff) & (df.date_day <= cutoff + timedelta(days=HORIZON_DAYS))]
    if train.empty or test.empty:
        return {}
    y = test[TARGET].to_numpy(dtype=float)
    results = {}

    naive = test["units_lag_7"].fillna(0).to_numpy(dtype=float)
    results["naive_lag7"] = {"wmape": wmape(y, naive), "bias": bias(y, naive)}

    ma = test["units_ma_28"].fillna(0).to_numpy(dtype=float)
    results["seasonal_ma28"] = {"wmape": wmape(y, ma), "bias": bias(y, ma)}

    # 500 rounds, no early stopping - this runs 6 times and train.py is where
    # the tuning happens. worth revisiting if the folds disagree a lot.
    model = lgb.train(
        {**LGB_PARAMS, "num_iterations": 500},
        lgb.Dataset(train[FEATURES], label=train[TARGET]),
    )
    pred = model.predict(test[FEATURES])
    results["lightgbm"] = {"wmape": wmape(y, pred), "bias": bias(y, pred)}
    return results


def main():
    mlflow.set_tracking_uri(mlflow_uri())
    mlflow.set_experiment("price_demand_backtest")
    df = load_features()
    cutoffs = fold_cutoffs(df)
    print(f"{len(df):,} rows, cutoffs: {[c.date().isoformat() for c in cutoffs]}")

    all_scores: dict[str, list[float]] = {}
    with mlflow.start_run(run_name="rolling_origin"):
        mlflow.log_params({"n_folds": N_FOLDS, "horizon_days": HORIZON_DAYS, **LGB_PARAMS})
        for k, cutoff in enumerate(cutoffs):
            for model_name, m in eval_fold(df, cutoff).items():
                with mlflow.start_run(run_name=f"fold{k}_{model_name}", nested=True):
                    mlflow.log_param("cutoff", cutoff.date().isoformat())
                    mlflow.log_param("model", model_name)
                    mlflow.log_metrics(m)
                all_scores.setdefault(model_name, []).append(m["wmape"])
                print(f"  fold {k} {model_name:14s} wmape={m['wmape']:.4f} bias={m['bias']:+.4f}")

        summary = {f"mean_wmape_{name}": float(np.mean(v)) for name, v in all_scores.items()}
        mlflow.log_metrics(summary)
        lgbm, naive = summary["mean_wmape_lightgbm"], summary["mean_wmape_naive_lag7"]
        improvement = 100 * (1 - lgbm / naive)
        mlflow.log_metric("pct_beat_naive", improvement)
        print(f"\nlightgbm wmape {lgbm:.4f} vs naive {naive:.4f} "
              f"({improvement:.1f}% better)")


if __name__ == "__main__":
    main()
