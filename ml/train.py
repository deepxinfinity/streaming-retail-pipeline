"""train the demand model and register it in mlflow.

poisson objective because that is how the simulator generates units. monotone
constraint forces predicted demand non-increasing in rel_price. only gets the
champion alias if it beats the incumbent's wmape.
"""

from datetime import timedelta

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
from mlflow import MlflowClient

from ml.common import (
    CHAMPION_ALIAS,
    FEATURES,
    LGB_PARAMS,
    MODEL_NAME,
    TARGET,
    bias,
    mlflow_uri,
    wmape,
)
from ml.dataset import load_features

VALID_DAYS = 14


def main():
    mlflow.set_tracking_uri(mlflow_uri())
    mlflow.set_experiment("price_demand_train")
    df = load_features()

    cutoff = df.date_day.max() - timedelta(days=VALID_DAYS)
    train, valid = df[df.date_day <= cutoff], df[df.date_day > cutoff]
    dtrain = lgb.Dataset(train[FEATURES], label=train[TARGET])
    dvalid = lgb.Dataset(valid[FEATURES], label=valid[TARGET], reference=dtrain)

    with mlflow.start_run(run_name="train") as run:
        mlflow.log_params(LGB_PARAMS)
        model = lgb.train(
            {**LGB_PARAMS, "num_iterations": 2000},
            dtrain,
            valid_sets=[dvalid],
            callbacks=[lgb.early_stopping(100), lgb.log_evaluation(100)],
        )
        pred = model.predict(valid[FEATURES])
        y = valid[TARGET].to_numpy(dtype=float)
        metrics = {
            "valid_wmape": wmape(y, pred),
            "valid_bias": bias(y, pred),
            "best_iteration": model.best_iteration,
        }
        mlflow.log_metrics(metrics)

        imp = sorted(zip(FEATURES, model.feature_importance("gain")),
                     key=lambda t: -t[1])
        mlflow.log_dict({k: float(v) for k, v in imp}, "feature_importance_gain.json")
        print("top features:", [f"{k}={v:,.0f}" for k, v in imp[:8]])

        mlflow.lightgbm.log_model(
            model, artifact_path="model", registered_model_name=MODEL_NAME,
            input_example=valid[FEATURES].head(5),
        )
        print(f"valid wmape={metrics['valid_wmape']:.4f} bias={metrics['valid_bias']:+.4f}")

        promote_if_better(run.info.run_id, metrics["valid_wmape"])


def promote_if_better(run_id: str, new_wmape: float) -> None:
    client = MlflowClient()
    version = next(
        v for v in client.search_model_versions(f"name='{MODEL_NAME}'") if v.run_id == run_id
    )
    try:
        # first ever run has no champion, mlflow raises rather than returning None
        champ = client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS)
        champ_wmape = client.get_run(champ.run_id).data.metrics.get("valid_wmape", float("inf"))
    except Exception:
        champ_wmape = float("inf")

    if new_wmape < champ_wmape:
        client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, version.version)
        print(f"promoted v{version.version} to @{CHAMPION_ALIAS} "
              f"({new_wmape:.4f} < incumbent {champ_wmape:.4f})")
    else:
        print(f"kept incumbent champion ({champ_wmape:.4f} <= {new_wmape:.4f})")


if __name__ == "__main__":
    main()
