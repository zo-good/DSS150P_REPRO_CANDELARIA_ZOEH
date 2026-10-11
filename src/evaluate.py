"""Evaluation and reproducibility check.

evaluate_model      predictions.csv -> outputs/metrics.json
reproducibility     metrics.json vs metadata/expected_metrics.json (within tolerance)
"""
import json

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, mean_absolute_error, mean_squared_error

from src.common import utc_now
from src.config import path_for


class ReproducibilityError(Exception):
    """The reproduced primary metric is outside the declared tolerance."""


def compute_metrics(preds):
    y, p = preds["severity"], preds["severity_pred"]
    per_class = f1_score(y, p, labels=["Low", "Moderate", "High"], average=None)
    reg = preds.dropna(subset=["cases_next"])
    return {
        "severity_accuracy": round(float(accuracy_score(y, p)), 4),
        "severity_macro_f1": round(float(f1_score(y, p, average="macro")), 4),
        "severity_f1_low": round(float(per_class[0]), 4),
        "severity_f1_moderate": round(float(per_class[1]), 4),
        "severity_f1_high": round(float(per_class[2]), 4),
        "nextweek_rf_mae": round(float(mean_absolute_error(reg["cases_next"], reg["cases_next_pred"])), 4),
        "nextweek_rf_rmse": round(float(np.sqrt(mean_squared_error(reg["cases_next"], reg["cases_next_pred"]))), 4),
        "nextweek_naive_mae": round(float(mean_absolute_error(reg["cases_next"], reg["naive_pred"])), 4),
        "nextweek_naive_rmse": round(float(np.sqrt(mean_squared_error(reg["cases_next"], reg["naive_pred"]))), 4),
    }


def evaluate_model(run_id, logger):
    preds = pd.read_csv(path_for("outputs_dir") / "predictions.csv")
    metrics = {"pipeline_run_id": run_id, "computed_at_utc": utc_now(),
               "test_rows": len(preds), **compute_metrics(preds)}
    (path_for("outputs_dir") / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    logger.info("metrics written", accuracy=metrics["severity_accuracy"],
                macro_f1=metrics["severity_macro_f1"], rf_mae=metrics["nextweek_rf_mae"],
                naive_mae=metrics["nextweek_naive_mae"])
    return metrics


def reproducibility_check(run_id, logger):
    expected = json.loads((path_for("metadata_dir") / "expected_metrics.json").read_text(encoding="utf-8"))
    actual = json.loads((path_for("outputs_dir") / "metrics.json").read_text(encoding="utf-8"))
    split = json.loads((path_for("metadata_dir") / "split_manifest.json").read_text(encoding="utf-8"))

    rows = []
    for name, exp in expected["metrics"].items():
        act = actual[name]
        rows.append({"metric": name, "expected": exp, "actual": act, "difference": round(act - exp, 4)})

    primary = expected["primary_metric"]
    diff = actual[primary["name"]] - primary["expected"]
    passed = abs(diff) <= primary["tolerance"]
    data_checks = {
        "split_date": [expected["data"]["split_date"], split["split_date"]],
        "train_rows": [expected["data"]["train_rows"], split["rows"]["train"]],
        "test_rows": [expected["data"]["test_rows"], split["rows"]["test"]],
        "rows_model_ready": [expected["data"]["rows_model_ready"], split["rows"]["curated"]],
    }
    data_ok = all(a == b for a, b in data_checks.values())
    report = {
        "pipeline_run_id": run_id, "checked_at_utc": utc_now(),
        "primary_metric": primary["name"], "expected": primary["expected"],
        "actual": actual[primary["name"]], "difference": round(diff, 4),
        "tolerance": primary["tolerance"], "passed": bool(passed and data_ok),
        "data_checks_expected_vs_actual": data_checks, "all_metrics": rows,
    }
    (path_for("outputs_dir") / "metrics_comparison.json").write_text(json.dumps(report, indent=2),
                                                                      encoding="utf-8")
    logger.info("reproducibility check", metric=primary["name"], expected=primary["expected"],
                actual=actual[primary["name"]], difference=round(diff, 4),
                tolerance=primary["tolerance"], data_ok=data_ok, passed=report["passed"])
    if not report["passed"]:
        raise ReproducibilityError(
            f"{primary['name']} differs by {diff:+.4f} (tolerance {primary['tolerance']}) "
            f"or split/row counts differ - see outputs/metrics_comparison.json")
    return report