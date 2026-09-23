from datetime import timedelta
from pathlib import Path

import mlflow
from evidently.metric_preset import DataDriftPreset
from evidently.report import Report

from ml.common import FEATURES, mlflow_uri
from ml.dataset import load_features

OUT = Path("reports/drift_report.html")
RECENT_DAYS = 14

# reference is everything older than RECENT_DAYS, current is the tail. on
# simulated data this should basically never fire - the generating process does
# not change. it is here so the nightly dag has the hook when the data is real.


def main():
    df = load_features()
    split = df.date_day.max() - timedelta(days=RECENT_DAYS)
    reference = df[df.date_day <= split][FEATURES].sample(min(100_000, len(df)), random_state=7)
    current = df[df.date_day > split][FEATURES]
    if current.empty:
        print("no recent rows to compare, skipping")
        return

    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=reference, current_data=current)
    OUT.parent.mkdir(exist_ok=True)
    report.save_html(str(OUT))

    mlflow.set_tracking_uri(mlflow_uri())
    mlflow.set_experiment("price_demand_drift")
    with mlflow.start_run(run_name="drift"):
        mlflow.log_artifact(str(OUT))
    print(f"drift report -> {OUT}")


if __name__ == "__main__":
    main()
