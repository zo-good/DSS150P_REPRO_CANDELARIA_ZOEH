"""Transformations: raw -> staging -> curated.

staging  typed, validated, deduplicated copies of each source (bad rows -> quarantine)
curated  weekly weather + dengue joined per city-week, plus engineered features

All logic is plain functions so it can be unit-tested and called from Airflow.
"""
import numpy as np
import pandas as pd
import yaml

from src.common import utc_now
from src.config import PROJECT_ROOT, SETTINGS, path_for

T = SETTINGS["transform"]
AUDIT_COLUMNS = ["pipeline_run_id", "processed_at_utc", "source_row_key", "record_hash"]


class DataContractError(Exception):
    """The data broke a rule whose contract action is 'fail'."""


def load_city_mapping():
    with open(PROJECT_ROOT / "config" / "city_mapping.yml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def quarantine(rows, reason, name, run_id, logger):
    """Append rejected rows (with a reason) to data/quarantine/<name>.csv."""
    if rows.empty:
        return
    qdir = path_for("quarantine_dir")
    qdir.mkdir(parents=True, exist_ok=True)
    out = qdir / f"{name}.csv"
    rows.assign(quarantine_reason=reason, pipeline_run_id=run_id).to_csv(
        out, mode="a", header=not out.exists(), index=False)
    logger.warning("rows quarantined", reason=reason, rows=len(rows), file=f"data/quarantine/{name}.csv")


# ---------------------------------------------------------------- staging

def clean_dengue(raw, run_id, logger):
    """Type-convert raw dengue rows; quarantine rows that break the data contract."""
    df = raw.copy()
    if T["strip_location_whitespace"]:
        df["loc"] = df["loc"].str.strip()
    df["cases"] = pd.to_numeric(df["cases"], errors="coerce")
    df["deaths"] = pd.to_numeric(df["deaths"].replace("", np.nan), errors="coerce")
    df["date"] = pd.to_datetime(df["date"], format=SETTINGS["source"]["dengue_date_format"],
                                errors="coerce")

    rules = {
        "missing or invalid cases": df["cases"].isna() | (df["cases"] < 0),
        "negative deaths": df["deaths"] < 0,
        "invalid date": df["date"].isna(),
        "date is not a Sunday (week end)": df["date"].notna() & (df["date"].dt.dayofweek != 6),
        "missing loc or Region": (df["loc"].str.strip() == "") | (df["Region"].str.strip() == ""),
    }
    bad = pd.Series(False, index=df.index)
    for reason, mask in rules.items():
        quarantine(raw[mask & ~bad], reason, "staging_dengue", run_id, logger)
        bad |= mask

    df = df[~bad]
    dupes = df.duplicated(subset=["loc", "date"], keep="first")
    quarantine(raw.loc[df.index[dupes]], "duplicate (loc, date)", "staging_dengue", run_id, logger)
    df = df[~dupes]

    if df["deaths"].isna().any():
        logger.warning("null deaths kept as null in staging", rows=int(df["deaths"].isna().sum()))
    df["cases"] = df["cases"].astype("int64")
    df["processed_at_utc"] = utc_now()
    df["pipeline_run_id"] = run_id
    return df.sort_values(["loc", "date"], kind="mergesort").reset_index(drop=True)


def clean_weather(raw, run_id, logger):
    """Type-convert daily weather; apply range rules; handle duplicate city-days per policy."""
    keep = ["city_name", "datetime", *T["weekly_sum_columns"], *T["weekly_mean_columns"]]
    df = raw[keep].copy()
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d", errors="coerce")
    ranges = {"precipitation_sum": (0, 1000), "rain_sum": (0, 1000),
              "temperature_2m_mean": (-5, 50), "temperature_2m_max": (-5, 55),
              "apparent_temperature_mean": (-10, 60)}
    bad = df["datetime"].isna() | df["city_name"].isna()
    for col, (lo, hi) in ranges.items():
        bad |= df[col].isna() | (df[col] < lo) | (df[col] > hi)
    quarantine(raw[bad], "weather value missing or out of contract range", "staging_weather",
               run_id, logger)
    df = df[~bad]

    dup = df.duplicated(subset=["city_name", "datetime"], keep=False)
    if dup.any():
        policy = T["weather_duplicate_policy"]
        logger.warning("duplicate city-days found", rows=int(dup.sum()), policy=policy,
                       cities=sorted(df.loc[dup, "city_name"].unique()))
        if policy == "average":
            df = df.groupby(["city_name", "datetime"], as_index=False, sort=False).mean()
        elif policy != "keep_all":
            raise DataContractError(f"unknown weather_duplicate_policy: {policy}")
    df["pipeline_run_id"] = run_id
    df["processed_at_utc"] = utc_now()
    return df.sort_values(["city_name", "datetime"], kind="mergesort").reset_index(drop=True)


# ---------------------------------------------------------------- curated

def weekly_weather(daily):
    """Daily -> Monday-to-Sunday weeks: sums for totals, means for temperatures/wind."""
    df = daily.copy()
    df["week_start"] = df["datetime"] - pd.to_timedelta(df["datetime"].dt.dayofweek, unit="D")
    agg = {c: "sum" for c in T["weekly_sum_columns"]}
    agg.update({c: "mean" for c in T["weekly_mean_columns"]})
    weekly = df.groupby(["city_name", "week_start"], as_index=False).agg(agg)
    if T["round_weekly_means"] is not None:
        weekly[T["weekly_mean_columns"]] = weekly[T["weekly_mean_columns"]].round(T["round_weekly_means"])
    weekly["week_end"] = weekly["week_start"] + pd.Timedelta(days=6)
    return weekly


def join_dengue_weather(dengue, weekly, logger=None):
    """Keep the mapped cities and attach the weather week that ends on each dengue date."""
    mapping = load_city_mapping()
    if logger is not None:
        locs, trimmed = set(dengue["loc"]), set(dengue["loc"].str.strip())
        lost = sorted(k for k in mapping if k not in locs and k in trimmed)
        if lost:
            logger.warning("mapped city dropped: its name only matches after trimming spaces",
                           cities=lost, setting="transform.strip_location_whitespace")
    d = dengue[dengue["loc"].isin(mapping)].copy()
    d["weather_city"] = d["loc"].map(mapping)
    d["week_start"] = d["date"] - pd.Timedelta(days=6)
    merged = d.merge(weekly, left_on=["weather_city", "week_start"],
                     right_on=["city_name", "week_start"], how="inner")
    merged["deaths"] = merged["deaths"].fillna(0)   # same rule as the group (nb 09)
    return merged.drop(columns=["weather_city", "city_name"])


def add_features(df):
    """Seasonal encodings, climate lags, rolling means and past cases, per city (nb 12)."""
    df = df.sort_values(["loc", "date"], kind="mergesort").reset_index(drop=True)
    g = df.groupby("loc")
    df["week_of_year"] = df["date"].dt.isocalendar().week.astype(int)
    df["month"] = df["date"].dt.month
    df["woy_sin"] = np.sin(2 * np.pi * df["week_of_year"] / 52)
    df["woy_cos"] = np.cos(2 * np.pi * df["week_of_year"] / 52)
    for col in T["lag_drivers"]:
        for lag in T["lags"]:
            df[f"{col}_lag{lag}"] = g[col].shift(lag)
    w = T["rolling_window"]
    for col in T["rolling_drivers"]:
        df[f"{col}_roll{w}"] = g[col].transform(lambda s: s.shift(1).rolling(w).mean())
    for lag in T["case_lags"]:
        df[f"cases_lag{lag}"] = g["cases"].shift(lag)
    df["cases_next"] = g["cases"].shift(-1)
    df["log_cases"] = np.log1p(df["cases"])

    # drop warm-up weeks where the longest lag / rolling window is still empty
    warm = [c for c in df.columns if c.endswith(f"lag{max(T['lags'])}")
            or c.endswith(f"roll{w}") or c == f"cases_lag{max(T['case_lags'])}"]
    return df.dropna(subset=warm).reset_index(drop=True)


def build_curated(dengue, daily_weather, run_id, logger=None):
    """Full curated build. Final order is deterministic: date, then loc."""
    curated = add_features(join_dengue_weather(dengue, weekly_weather(daily_weather), logger))
    curated["pipeline_run_id"] = run_id
    curated["processed_at_utc"] = utc_now()
    return curated.sort_values(["date", "loc"], kind="mergesort").reset_index(drop=True)


# ---------------------------------------------------------------- pipeline steps

def run_staging(run_id, logger):
    """raw -> staging: read every committed raw file, clean it, write Parquet."""
    raw_dir, stg_dir = path_for("raw_dir"), path_for("staging_dir")
    stg_dir.mkdir(parents=True, exist_ok=True)

    batch_files = sorted((raw_dir / "dengue_batches").glob("*.csv"))
    if not batch_files:
        raise FileNotFoundError("no raw dengue batches found - run: python -m src.cli ingest")
    raw = pd.concat([pd.read_csv(f, dtype=str, keep_default_na=False) for f in batch_files],
                    ignore_index=True)
    dengue = clean_dengue(raw, run_id, logger)
    dengue.to_parquet(stg_dir / "dengue_weekly.parquet", index=False)
    logger.info("staging written", table="dengue_weekly", rows_in=len(raw), rows_out=len(dengue))

    weather_files = [raw_dir / "weather" / n for n in SETTINGS["source"]["weather_files"]]
    raw_w = pd.concat([pd.read_csv(f) for f in weather_files], ignore_index=True)
    weather = clean_weather(raw_w, run_id, logger)
    weather.to_parquet(stg_dir / "weather_daily.parquet", index=False)
    logger.info("staging written", table="weather_daily", rows_in=len(raw_w), rows_out=len(weather))
    return dengue, weather


def run_curated(run_id, logger):
    """staging -> curated: weekly weather, join, features. Written as Parquet and CSV."""
    stg_dir, cur_dir = path_for("staging_dir"), path_for("curated_dir")
    cur_dir.mkdir(parents=True, exist_ok=True)
    dengue = pd.read_parquet(stg_dir / "dengue_weekly.parquet")
    weather = pd.read_parquet(stg_dir / "weather_daily.parquet")
    curated = build_curated(dengue, weather, run_id, logger)

    dupes = int(curated.duplicated(subset=["loc", "date"]).sum())
    if dupes:
        raise DataContractError(f"curated has {dupes} duplicate (loc, date) rows")
    curated.to_parquet(cur_dir / "dengue_weather_weekly.parquet", index=False)
    curated.to_csv(cur_dir / "dengue_weather_weekly.csv", index=False)
    logger.info("curated written", table="dengue_weather_weekly", rows=len(curated),
                cities=int(curated["loc"].nunique()),
                first_date=str(curated["date"].min().date()), last_date=str(curated["date"].max().date()))
    return curated