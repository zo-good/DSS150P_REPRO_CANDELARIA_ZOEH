"""Data-quality checks for the raw and curated layers, driven by metadata/data_contract.yml.

Every check returns a dict: name, category, passed, detail, action.
If any check with action 'fail' does not pass, DataQualityError stops the pipeline
(nonzero exit code), so bad data cannot silently reach the model.
Results are saved to outputs/quality/<layer>_checks.json.
"""
import json

import pandas as pd
import yaml

from src.common import sha256_of, utc_now
from src.config import PROJECT_ROOT, SETTINGS, path_for, source_file

REQUIRED_CURATED = ["loc", "date", "Region", "cases", "deaths", "week_start", "week_end",
                    "source_row_key", "record_hash", "source_checksum"]


class DataQualityError(Exception):
    """At least one 'fail' data-quality check did not pass."""


def load_contract():
    with open(path_for("metadata_dir") / "data_contract.yml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def check(name, category, passed, detail, action="fail"):
    return {"name": name, "category": category, "passed": bool(passed),
            "detail": detail, "action": action}


def enforce(results, layer, run_id, logger):
    """Log every check, save them as JSON, and stop if a 'fail' check did not pass."""
    for r in results:
        level = logger.info if r["passed"] else (logger.error if r["action"] == "fail" else logger.warning)
        level("quality check", layer=layer, check=r["name"], category=r["category"],
              passed=r["passed"], detail=r["detail"])
    out = path_for("outputs_dir") / "quality"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{layer}_checks.json").write_text(json.dumps(
        {"pipeline_run_id": run_id, "checked_at_utc": utc_now(), "layer": layer,
         "passed": sum(r["passed"] for r in results), "total": len(results), "checks": results},
        indent=2, default=str), encoding="utf-8")
    failed = [r["name"] for r in results if not r["passed"] and r["action"] == "fail"]
    if failed:
        raise DataQualityError(f"{layer}: {len(failed)} check(s) failed: {', '.join(failed)}")
    return results


# ------------------------------------------------------------------ raw layer

def raw_checks():
    contract = load_contract()["sources"]["dengue"]
    batch_dir = path_for("raw_dir") / "dengue_batches"
    files = sorted(batch_dir.glob("*.csv"))
    raw = pd.concat([pd.read_csv(f, dtype=str, keep_default_na=False) for f in files],
                    ignore_index=True) if files else pd.DataFrame()
    q = path_for("quarantine_dir") / "ingest_unparseable_date.csv"
    quarantined = len(pd.read_csv(q)) if q.exists() else 0
    checkpoint = json.loads((PROJECT_ROOT / SETTINGS["ingestion"]["checkpoint_file"]).read_text())
    source_rows = sum(1 for _ in open(source_file(contract["file"]), "rb")) - 1
    actual_sha = sha256_of(source_file(contract["file"]))
    missing = [c for c in contract["required_columns"] if c not in raw.columns]
    audit = ["pipeline_run_id", "ingested_at_utc", "source_file", "source_checksum"]
    return [
        check("source_checksum_matches_contract", "freshness/applicability",
              actual_sha == contract["sha256"], f"source sha256 {actual_sha[:12]}..."),
        check("raw_batches_exist", "completeness", len(files) > 0, f"{len(files)} batch files"),
        check("required_columns_present", "schema", not missing, f"missing: {missing or 'none'}"),
        check("raw_plus_quarantine_equals_source", "completeness",
              len(raw) + quarantined == source_rows,
              f"raw {len(raw)} + quarantined {quarantined} vs source {source_rows}"),
        check("source_row_key_unique", "uniqueness",
              len(raw) > 0 and not raw["source_row_key"].duplicated().any(),
              f"{int(raw['source_row_key'].duplicated().sum()) if len(raw) else 'n/a'} duplicate keys"),
        check("audit_columns_filled", "completeness",
              all(c in raw and (raw[c] != "").all() for c in audit), f"columns: {audit}"),
        check("checkpoint_matches_raw_rows", "consistency",
              sum(b["rows"] for b in checkpoint["committed_batches"]) == len(raw),
              f"checkpoint rows {sum(b['rows'] for b in checkpoint['committed_batches'])} vs raw {len(raw)}"),
        check("min_rows", "completeness", len(raw) >= contract["min_rows"],
              f"{len(raw)} rows (minimum {contract['min_rows']})"),
    ]


def run_validate_raw(run_id, logger):
    return enforce(raw_checks(), "raw", run_id, logger)


# ------------------------------------------------------------------ curated layer

def curated_checks(df):
    """Checks on the curated (model-ready) table. Works on any DataFrame, so it is unit-testable."""
    contract = load_contract()
    cur, dengue = contract["curated"], contract["sources"]["dengue"]
    features = SETTINGS["features"]["numeric"]
    missing = [c for c in REQUIRED_CURATED + features if c not in df.columns]
    if missing:   # nothing else can be checked without the columns
        return [check("required_columns_present", "schema", False, f"missing: {missing}")]

    key = cur["primary_key"]
    s = df.sort_values(["loc", "date"], kind="mergesort")
    prev_cases = s.groupby("loc")["cases"].shift(1)
    consecutive = s.groupby("loc")["date"].diff() == pd.Timedelta(days=7)
    lag_ok = (s.loc[consecutive, "cases_lag1"] == prev_cases[consecutive]).all()
    feature_nulls = int(df[features].isna().sum().sum())
    leaked = sorted(set(features) & set(cur["forbidden_features"]))
    return [
        check("required_columns_present", "schema", True, f"{len(REQUIRED_CURATED + features)} columns"),
        check("types_parse", "schema",
              pd.api.types.is_datetime64_any_dtype(df["date"]) and pd.api.types.is_integer_dtype(df["cases"]),
              f"date={df['date'].dtype}, cases={df['cases'].dtype}"),
        check("key_and_target_not_null", "completeness",
              int(df[key + ["cases"]].isna().sum().sum()) == 0, "loc, date, cases have no nulls"),
        check("features_not_null", "completeness", feature_nulls == 0,
              f"{feature_nulls} null feature values (warm-up weeks are dropped)"),
        check("primary_key_unique", "uniqueness", not df.duplicated(subset=key).any(),
              f"{int(df.duplicated(subset=key).sum())} duplicate {key}"),
        check("cases_and_deaths_non_negative", "validity/range",
              bool((df["cases"] >= 0).all() and (df["deaths"] >= 0).all()),
              f"min cases {df['cases'].min()}, min deaths {df['deaths'].min()}"),
        check("date_is_week_end_sunday", "validity/format",
              bool((df["date"].dt.dayofweek == 6).all() and (df["date"] == df["week_end"]).all()),
              "every date is a Sunday and equals week_end"),
        check("week_is_seven_days", "consistency",
              bool(((df["week_end"] - df["week_start"]) == pd.Timedelta(days=6)).all()),
              "week_end - week_start = 6 days"),
        check("cases_lag1_equals_previous_week", "consistency", bool(lag_ok),
              "cases_lag1 == last week's cases for every consecutive city-week"),
        check("expected_city_count", "business rule", df["loc"].nunique() == cur["expected_cities"],
              f"{df['loc'].nunique()} cities (contract expects {cur['expected_cities']})"),
        check("no_forbidden_features", "predictive integrity", not leaked,
              f"forbidden columns in feature list: {leaked or 'none'}"),
        check("built_from_contract_source", "freshness/applicability",
              bool((df["source_checksum"] == dengue["sha256"]).all()),
              "every row came from the source file named in the contract"),
    ]


def postgres_checks(n_rows):
    """The PostgreSQL table holds exactly the curated rows, with no duplicate keys."""
    from src.storage import connect
    conn = connect(retries=2, wait_seconds=1)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), count(DISTINCT (loc, date)) FROM curated_dengue_weekly")
            total, distinct = cur.fetchone()
    finally:
        conn.close()
    return [
        check("postgres_rows_match_curated_file", "consistency", total == n_rows,
              f"postgres {total} vs curated file {n_rows}"),
        check("postgres_keys_unique", "uniqueness", total == distinct,
              f"{total - distinct} duplicate keys in postgres"),
    ]


def run_validate_curated(run_id, logger):
    df = pd.read_parquet(path_for("curated_dir") / "dengue_weather_weekly.parquet")
    return enforce(curated_checks(df) + postgres_checks(len(df)), "curated", run_id, logger)