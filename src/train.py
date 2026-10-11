"""Model input + training: curated -> deterministic split -> random forests.

Reproduces the group's notebook 12: chronological 80/20 split, per-city severity
thresholds from the TRAINING period only, RandomForest(300 trees, random_state=42).
"""
import hashlib
import json
import random

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor

from src.common import utc_now
from src.config import SETTINGS, path_for

M, FEAT = SETTINGS["model"], SETTINGS["features"]
FORBIDDEN = {"cases", "cases_next", "log_cases", "deaths", "severity"}


class LeakageError(Exception):
    """A forbidden column (target or same-week information) was used as a feature."""


def set_seeds(seed):
    """Fix every random number generator the project uses."""
    random.seed(seed)
    np.random.seed(seed)


def keys_hash(frame):
    """Fingerprint of the ordered (loc, date) keys: proves which rows, in which order."""
    keys = frame["loc"] + "|" + frame["date"].dt.strftime("%Y-%m-%d")
    return hashlib.sha256("\n".join(keys).encode("utf-8")).hexdigest()


def group_row_order(df):
    """Row order exactly as in the group's notebook 12 (sort by loc,date then by date).

    A random forest draws bootstrap samples by row position, so the row order is part
    of the model's behaviour. The second sort uses pandas' default algorithm, exactly
    like the group's code; split_manifest.json records a hash of the result so any
    change in order is detected by the reproducibility check.
    """
    df = df.sort_values(["loc", "date"]).reset_index(drop=True)
    return df.sort_values("date").reset_index(drop=True)


def split_and_label(df):
    """Chronological split + severity labels from training-period thresholds only."""
    cut = df["date"].quantile(M["train_fraction"])
    df = df.assign(split=np.where(df["date"] <= cut, "train", "test"))
    train = df[df["split"] == "train"]
    q_low, q_high = M["severity_quantiles"]
    thr = train.groupby("loc")["cases"].quantile([q_low, q_high]).unstack()
    low = df["loc"].map(thr[q_low])
    high = df["loc"].map(thr[q_high])
    df["severity"] = np.where(df["cases"] <= low, "Low",
                              np.where(df["cases"] <= high, "Moderate", "High"))
    return df, cut, thr


def design_matrix(frame, columns=None):
    """Numeric features + one-hot city columns, aligned to the training columns."""
    X = pd.get_dummies(frame[FEAT["numeric"] + FEAT["one_hot"]], columns=FEAT["one_hot"])
    return X if columns is None else X.reindex(columns=columns, fill_value=0)


def check_no_leakage():
    used = set(FEAT["numeric"]) | set(FEAT["one_hot"])
    leaked = used & FORBIDDEN
    if leaked:
        raise LeakageError(f"forbidden columns used as features: {sorted(leaked)}")


def run_training(run_id, logger):
    set_seeds(M["random_state"])
    check_no_leakage()
    curated = pd.read_parquet(path_for("curated_dir") / "dengue_weather_weekly.parquet")
    df, cut, thr = split_and_label(group_row_order(curated))
    train, test = df[df["split"] == "train"], df[df["split"] == "test"]
    if train["date"].max() >= test["date"].min():
        raise LeakageError("train and test periods overlap")

    # Model A: severity classifier (primary result)
    X_train = design_matrix(train)
    X_test = design_matrix(test, X_train.columns)
    clf = RandomForestClassifier(n_estimators=M["n_estimators"], random_state=M["random_state"],
                                 n_jobs=M["n_jobs"])
    clf.fit(X_train, train["severity"])
    test = test.assign(severity_pred=clf.predict(X_test))

    # Model B: next-week cases regressor vs naive "same as this week" baseline
    tr2 = train.dropna(subset=["cases_next"])
    te2 = test.dropna(subset=["cases_next"])
    X2 = design_matrix(tr2)
    reg = RandomForestRegressor(n_estimators=M["n_estimators"], random_state=M["random_state"],
                                n_jobs=M["n_jobs"])
    reg.fit(X2, tr2["cases_next"])
    test.loc[te2.index, "cases_next_pred"] = reg.predict(design_matrix(te2, X2.columns))
    test["naive_pred"] = test["cases"]

    out_dir = path_for("outputs_dir")
    (out_dir / "model").mkdir(parents=True, exist_ok=True)
    joblib.dump({"classifier": clf, "regressor": reg, "feature_columns": list(X_train.columns)},
                out_dir / "model" / "random_forests.joblib", compress=3)

    preds = test[["loc", "date", "cases", "severity", "severity_pred",
                  "cases_next", "cases_next_pred", "naive_pred"]].copy()
    preds["date"] = preds["date"].dt.strftime("%Y-%m-%d")
    preds["pipeline_run_id"] = run_id
    preds.to_csv(out_dir / "predictions.csv", index=False)

    manifest = {
        "pipeline_run_id": run_id, "created_at_utc": utc_now(),
        "split_method": "chronological; train = date <= quantile(date, train_fraction)",
        "train_fraction": M["train_fraction"], "split_date": str(cut.date()),
        "row_order": "group notebook 12: sort_values(['loc','date']) then sort_values('date')",
        "random_state": M["random_state"],
        "rows": {"curated": len(df), "train": len(train), "test": len(test),
                 "train_regression": len(tr2), "test_regression": len(te2)},
        "train_dates": [str(train["date"].min().date()), str(train["date"].max().date())],
        "test_dates": [str(test["date"].min().date()), str(test["date"].max().date())],
        "train_keys_sha256": keys_hash(train), "test_keys_sha256": keys_hash(test),
        "model_input_order_sha256": keys_hash(df),
        "n_features": X_train.shape[1], "feature_columns": list(X_train.columns),
        "severity_thresholds": {loc: [float(r.iloc[0]), float(r.iloc[1])] for loc, r in thr.iterrows()},
    }
    (path_for("metadata_dir") / "split_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("models trained", split_date=manifest["split_date"], train_rows=len(train),
                test_rows=len(test), n_features=X_train.shape[1])
    return manifest