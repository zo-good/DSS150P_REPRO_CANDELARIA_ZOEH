# Reproducibility Report: Dengue Severity Prediction (DSS150P Lab 4)

**Student:** Zoeh Candelaria · **Repository:** https://github.com/zo-good/DSS150P_REPRO_CANDELARIA_ZOEH
**Prior project:** group Predictive Analytics project `dengue-ph-prediction` (Candelaria, Santos, Vicente)

## 1. Summary

The group's final 41-city severity model was rebuilt as a reproducible pipeline that starts
from the three original source files. The pipeline regenerates the group's model-ready dataset
**exactly** (10,433 rows; every one of 49 compared columns identical) and retrains the model
to **macro-F1 0.4972 vs. the reported 0.494** (difference +0.0032, declared tolerance ±0.01:
**PASS**). The same 0.4972 was obtained on Windows (venv, Python 3.14), Linux (Docker,
Python 3.12) and inside Airflow.

## 2. What the prior project did (baseline)

| Item | Prior project |
|---|---|
| Sources | DOH weekly dengue CSV (HDX) + two daily weather CSVs (Open-Meteo format) |
| Code | 14 Jupyter notebooks, run by hand in order, linked through CSVs in `data/processed/` |
| Final notebook | `12_all_cities_timeseries.ipynb` (41 cities); `14_all_cities_modeling.ipynb` re-runs it |
| Target | `severity` = Low / Moderate / High: per-city 33rd/66th percentile of weekly `cases`, thresholds from the training period |
| Features | 45: 16 weekly weather fields, week/month seasonality, 5 drivers × lags 1-4, two 4-week rolling means, `cases_lag1-3`; plus one-hot city |
| Split | chronological, train = date ≤ 80th-percentile date (2020-01-12): 8,373 train / 2,060 test |
| Model | `RandomForestClassifier(n_estimators=300, random_state=42, n_jobs=-1)`; secondary `RandomForestRegressor` for next-week cases |
| Reported metrics | accuracy 0.717, **macro-F1 0.494** (primary); RF MAE 5.87 vs naive MAE 3.83 |
| Recorded in | `metadata/expected_metrics.json` (machine-readable baseline) |

**Lifecycle of the prior project:** *generation* (DOH surveillance, Open-Meteo) → *ingestion*
(manual download into `data/raw/`) → *storage* (CSV files only) → *transformation* (notebooks
05–12, intermediate CSVs) → *serving* (in-notebook printouts) → *consumers* (the group's paper
and presentation).

## 3. Engineering gap table

| # | Current behaviour in the prior project | Risk | DSS150P concept | Remediation in this repository |
|---|---|---|---|---|
| 1 | Relative paths like `../data/processed/...` that only work from `notebooks/` | cannot run elsewhere | reproducible environments | `src/config.py` builds every path from the project root; `config/settings.yml` |
| 2 | Notebooks run by hand in numbered order; hidden notebook state | wrong order or stale outputs give silently different results | modular code, orchestration | `src/` modules + `python -m src.cli <step>`; Airflow DAG with explicit dependencies |
| 3 | `weather_2016_2023_weekly.csv` has **no generating notebook** (manual, undocumented step) | the dataset cannot be rebuilt from source | lineage, ETL | weekly aggregation reverse-engineered and verified exactly (7 fields summed, 9 averaged, 2 dp); `transform.weekly_weather` |
| 4 | No dependency file; versions unknown | different numbers on another machine | dependency pinning | `requirements.txt` with exact versions; Dockerfile; separate venv inside Airflow |
| 5 | Source files not identified or checked | a changed file goes unnoticed | source contracts, checksums | `source_manifest.yml` with SHA-256 + row counts; `verify-source`; checksum check on every run |
| 6 | HDX tag row (`#adm2+name`) loaded as data | numeric columns parsed as text | schema validation, quarantine | tag row quarantined at ingestion with a reason |
| 7 | `" CAGAYAN DE ORO CITY"` (leading space) never matched the mapping: city **silently dropped** | silent data loss | data quality, observability | reproduced for fidelity, but **logged as a warning**; fix = `strip_location_whitespace: true` |
| 8 | 4 weather cities duplicated per day in 2020-2023; both copies summed: **Lucena 2020 rainfall about doubled** | biased features | uniqueness checks | duplicate check + warning; policy setting `keep_all` (reproduce) or `average` (fix) |
| 9 | Unstable `sort_values('date')` and row order not recorded | random-forest result depends on row order (0.497 vs 0.505) | determinism | group's exact sort reproduced; order fingerprint (`model_input_order_sha256`) in `split_manifest.json` |
| 10 | Metrics only visible as notebook printouts; notebook 14's summary (~0.73 / ~0.52) disagrees with notebook 12's output (0.717 / 0.494) | no trustworthy baseline | machine-readable outputs | `outputs/metrics.json` + automatic comparison `metrics_comparison.json`; notebook 12 output chosen as baseline (actual code output) |
| 11 | No checks between steps | bad data reaches the model unnoticed | data quality testing | 22 contract-driven checks; curated quality gate before writing; failure injection demo |
| 12 | Re-running steps overwrites or appends without control | duplicates | idempotency, incremental loads | checkpoint/watermark ingestion; `ON CONFLICT` upsert; reruns add nothing |
| 13 | `.DS_Store` committed; no `.gitignore` for data | noise, oversized repo | version control hygiene | `.gitignore`, `.gitattributes` (keeps CSV bytes identical across OSes) |
| 14 | One missing `deaths` value (QUEZON CITY, 2017-05-07) filled silently | hidden imputation | data contracts | contract `on_null: warn`; kept null in staging, filled 0 in curated (group rule), documented |

## 4. Metric consistency

| Metric | Expected (group) | Reproduced | Difference |
|---|---:|---:|---:|
| **severity macro-F1 (primary)** | **0.494** | **0.4972** | **+0.0032** |
| severity accuracy | 0.717 | 0.7184 | +0.0014 |
| F1 Low / Moderate / High | 0.86 / 0.37 / 0.26 | 0.8600 / 0.3711 / 0.2606 | 0.0000 / +0.0011 / +0.0006 |
| next-week RF MAE | 5.87 | 5.8981 | +0.0281 |
| next-week RF RMSE | 18.82 | 19.2486 | +0.4286 |
| naive MAE / RMSE | 3.83 / 7.73 | 3.8307 / 7.7276 | +0.0007 / −0.0024 |

**Tolerance rule:** primary metric within **±0.01** of the baseline (the lab's recommended
maximum), **and** identical split date, train/test row counts and model-ready row count.
Checked automatically by `reproducibility-check`; the result is in `outputs/metrics_comparison.json`.

**The data is identical:** the naive baseline (next week = this week) uses no model, only the
data, and it matches to rounding (3.8307 vs 3.83). Together with the identical row keys, this
shows that train and test contain the same logical rows and target values as the baseline.
The remaining small differences come from the random forest, not from the data.

## 5. Deviations and their causes

**5.1 Macro-F1 +0.0032 (within tolerance).** The data and row order are identical, so the
difference comes from the library version, which the prior project never recorded. Running the
group's **own saved model-ready file** under different scikit-learn versions:

| scikit-learn | group row order | (loc, date) row order |
|---|---:|---:|
| 1.5.2 | 0.4972 | 0.5046 |
| 1.6.1 | 0.4991 | 0.5104 |
| 1.7.2 | 0.4991 | 0.5104 |
| 1.9.1 (pinned here) | **0.4972** | 0.5054 |

No tested version reproduces 0.494 exactly, so the group probably used another version.
Every version is within tolerance **only if the group's row order is kept**, which is why the
pipeline reproduces that order.

**5.2 RF RMSE +0.43.** RMSE is dominated by a few large errors in outbreak cities, so it is the
metric most sensitive to small changes in individual trees. It is a secondary metric; MAE
differs by only 0.03.

**5.3 Group bugs reproduced on purpose.** Fidelity first: the pipeline reproduces the
Cagayan de Oro drop and the summed duplicate weather rows, but logs both as warnings and
exposes each fix as a setting. Effect of the fixes (seed 42, same pipeline):

| Variant | macro-F1 | Rows / cities |
|---|---:|---|
| reproduce group (default) | 0.4972 | 10,433 / 41 |
| fix duplicate weather (`average`) | 0.5010 | 10,433 / 41 |
| fix whitespace (`strip_location_whitespace: true`) | 0.4997 | 10,689 / 42 |
| fix both | 0.4961 | 10,689 / 42 |

The fixes move macro-F1 by at most 0.004, but the whitespace fix changes the dataset itself
(42 cities), which a reproduction cannot accept. For a real production system the fixed
configuration would be the correct default.

**5.4 Scope.** Reproduced: notebook 12's full-period severity classifier and next-week
regressor. Not reproduced: the SARIMA benchmark (notebook 14 part C), the pre-COVID 2019 test
window, and the separate NCR notebook (13). These are side analyses, not the reported baseline.

## 6. Deterministic controls

- **Fixed source:** SHA-256 and row counts verified on every run; `.gitattributes` stops
  line-ending conversion from changing bytes on Windows.
- **Fixed environment:** exact package versions; the Docker image; Airflow's own packages kept
  in a separate venv, so Airflow's numpy/pyarrow/SQLAlchemy pins can't change model behaviour.
- **Fixed transformations:** stable (`mergesort`) sorts; per-city lags computed from past
  weeks only; all parameters in `config/settings.yml`.
- **Fixed split and labels:** chronological cut, thresholds from training rows only (unit-tested);
  `split_manifest.json` stores the cut date, row counts and SHA-256 fingerprints of the train
  keys, test keys and model-input row order.
- **Fixed model:** `random_state=42`, Python and numpy seeds set; feature list declared explicitly.
- **Cross-platform change detection:** decimals rounded to 6 places before row hashing, because
  Windows and Linux numpy builds differ around 1e-14 (found when a Docker load reported 408
  "changed" rows; see `docs/evidence/cross_platform_hash_fix.txt`).

## 7. Remaining nondeterminism and limitations

- **Seed sensitivity:** across seeds 0-19, macro-F1 has mean 0.5073, sd 0.0057, range
  0.4985-0.5190; only 35% of seeds fall within ±0.01 of 0.494. The ±0.01 tolerance therefore
  only holds for the declared seed 42. The baseline itself is one draw from that spread.
- **Unknown original environment:** the group's library versions were never recorded (see 5.1).
- **Weather provenance:** the exact download page of the weather files was not recorded by the
  group; the files are pinned by checksum and mirrored on Google Drive.
- **Model limitations (inherited):** single chronological split; the test period (2020) is
  affected by COVID-era reporting; at a 1-week horizon the naive baseline beats the random
  forest (MAE 3.83 vs 5.90).

## 8. How to verify (acceptance tests)

| ID | Test | Evidence |
|---|---|---|
| A1 | Clones and README setup is sufficient | `README.md` (clean-machine steps) |
| A2 | Source identity verified | `python -m src.cli verify-source`; `metadata/source_manifest.yml` |
| A3 | No machine-specific absolute paths | `src/config.py`; all paths relative to the project root |
| A4 | No secrets committed | `.gitignore` (`.env`), `.env.example` only |
| A5 | Environment from declared files | `requirements.txt`, `Dockerfile`, `Dockerfile.airflow`, `docker-compose.yml`; `docs/evidence/docker_run_all.txt` |
| A6 | Full ingestion gives expected raw count/checksum | `ingest --mode full`; raw 32,701 + 1 quarantined = 32,702 |
| A7 | Incremental/checkpoint avoids duplicates | `docs/evidence/ingestion_recovery_demo.txt`, `checkpoint_after_recovery_demo.json` |
| A8 | Staging/curated deterministic | identical curated output across 3 environments; integration test |
| A9 | PostgreSQL load rerun-safe | `docs/evidence/postgres_rerun_demo.txt`; `load_audit` table |
| A10 | Format comparison uses same records | `docs/benchmark_report.md` ("Same records: yes") |
| A11 | Partition read independently | `docs/benchmark_report.md` (year=2019: 2,091 rows OK) |
| A12 | Airflow DAG completes end-to-end | `docs/evidence/airflow_success_graph.png`, `airflow_success_log.png` |
| A13 | Recoverable failure demonstrated | `docs/evidence/failure_injection_demo.txt`, `airflow_failed_*.png`, `airflow_failures.jsonl` |
| A14 | Quality checks detect invalid data | 3 injections blocked; `tests/test_integration.py` |
| A15 | Second run adds no duplicate keys | `batches_added=0`, `inserted=0 updated=0`; `postgres_keys_unique` check |
| A16 | Metrics regenerated from curated data | `src/train.py` reads `data/curated/`; `split_manifest.json` |
| A17 | Reproduced metric passes tolerance | `outputs/metrics_comparison.json` (`"passed": true`) |
| A18 | Docs consistent with implementation | `docs/architecture.png`, `docs/lineage.png` (sources in `docs/diagrams/`), `docs/runbook.md` |