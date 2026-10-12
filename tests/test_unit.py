"""Unit tests: small, fast checks of single reusable functions (no files, no database)."""
import numpy as np
import pandas as pd
import pytest

from src import ingest, storage, train, transform


def test_row_keys_are_deterministic_and_content_based():
    raw = pd.DataFrame({"loc": ["A CITY", "B CITY"], "cases": ["3", "5"], "deaths": ["0", ""],
                        "date": ["1/10/2016", "1/10/2016"], "Region": ["R1", "R2"]})
    first = ingest.add_row_keys(raw.copy())
    second = ingest.add_row_keys(raw.copy())
    assert first["record_hash"].tolist() == second["record_hash"].tolist()      # same input, same hash
    assert first["source_row_key"].tolist() == ["A CITY|1/10/2016", "B CITY|1/10/2016"]
    changed = raw.copy()
    changed.loc[0, "cases"] = "4"
    assert ingest.add_row_keys(changed)["record_hash"][0] != first["record_hash"][0]


def test_weekly_weather_sums_totals_and_averages_temperatures():
    days = pd.date_range("2016-01-04", periods=7, freq="D")            # Monday..Sunday
    daily = pd.DataFrame({"city_name": "X", "datetime": days})
    for col in transform.T["weekly_sum_columns"]:
        daily[col] = 1.0
    for col in transform.T["weekly_mean_columns"]:
        daily[col] = np.arange(7, dtype=float)                       # 0..6, mean 3
    weekly = transform.weekly_weather(daily)
    assert len(weekly) == 1
    row = weekly.iloc[0]
    assert row["week_start"] == pd.Timestamp("2016-01-04")
    assert row["week_end"] == pd.Timestamp("2016-01-10")
    assert row["rain_sum"] == 7.0                                      # summed
    assert row["temperature_2m_mean"] == 3.0                           # averaged


def test_severity_thresholds_use_training_period_only():
    dates = pd.date_range("2016-01-03", periods=10, freq="7D")
    df = pd.DataFrame({"loc": "X", "date": dates, "cases": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    labelled, cut, thr = train.split_and_label(df)
    huge_test = df.copy()
    huge_test.loc[huge_test["date"] > cut, "cases"] = 10_000            # change only test weeks
    _, _, thr_after = train.split_and_label(huge_test)
    pd.testing.assert_frame_equal(thr, thr_after)                     # thresholds did not move
    assert set(labelled["severity"]) <= {"Low", "Moderate", "High"}


def test_leakage_guard_rejects_forbidden_feature(monkeypatch):
    monkeypatch.setitem(train.FEAT, "numeric", [*train.FEAT["numeric"], "cases"])
    with pytest.raises(train.LeakageError):
        train.check_no_leakage()


def test_row_hash_ignores_float_noise_but_catches_real_changes():
    cols = ["loc", "date", "region", "cases", "deaths", "week_start", "week_end", "rain_sum",
            "source_row_key", "record_hash", "row_hash", "pipeline_run_id", "processed_at_utc"]
    df = pd.DataFrame({"loc": ["A"], "date": [pd.Timestamp("2016-01-10")], "Region": ["R"],
                       "cases": [3], "deaths": [0.0], "week_start": [pd.Timestamp("2016-01-04")],
                       "week_end": [pd.Timestamp("2016-01-10")], "rain_sum": [1.45],
                       "source_row_key": ["A|1/10/2016"], "record_hash": ["h"],
                       "pipeline_run_id": ["r1"], "processed_at_utc": ["2026-01-01T00:00:00+00:00"]})
    base = storage.prepare_rows(df, cols)["row_hash"][0]
    noisy = df.assign(rain_sum=1.45 + 1e-14, pipeline_run_id="r2")       # noise + a new run id
    assert storage.prepare_rows(noisy, cols)["row_hash"][0] == base
    real = df.assign(rain_sum=2.45)
    assert storage.prepare_rows(real, cols)["row_hash"][0] != base