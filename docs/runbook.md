# Runbook: Dengue Reproduction Pipeline

Operational guide: how to run the pipeline, what normal looks like, and how to recover.
Commands assume the project root and an active venv (or use `docker compose run --rm pipeline ...`).

## 1. Normal execution

| How | Command |
|---|---|
| Airflow | `docker compose up -d airflow` -> http://localhost:8080 -> trigger `predictive_repro_pipeline` |
| Docker only | `docker compose up -d postgres` then `docker compose run --rm pipeline` |
| Local | `docker compose up -d postgres` then `python -m src.cli run-all` |

**A healthy run** ends with `reproducibility check ... passed=True` and
`run summary published status=success`. On a rerun with no new data, expect
`batches_added=0` and `inserted=0 updated=0 unchanged=10433`. Warnings that are **expected**
on every run (known, documented source issues, not failures):

- `rows quarantined at ingestion rows=1`: the HDX HXL label row
- `null deaths kept as null in staging rows=1`: QUEZON CITY 2017-05-07
- `duplicate city-days found ... policy=keep_all`: 4 weather cities duplicated in 2020-2023
- `mapped city dropped ... CAGAYAN DE ORO CITY`: leading space in the source name

Where to look: terminal output, `logs/pipeline.jsonl` (one JSON line per event, filter by
`pipeline_run_id`), `outputs/run_summary.json`, Airflow task logs, and in PostgreSQL the
`load_audit` and `pipeline_run_summary` tables.

## 2. Common failures

| Failing step | Error | What it means | Fix |
|---|---|---|---|
| ingest | `SourceChecksumError` | source bytes differ from `metadata/source_manifest.yml` | re-download the file; never edit sources. Only update the manifest for a deliberate new source version |
| ingest | `FileNotFoundError: source file missing` | weather files not placed | see README, setup step 2 |
| validate-raw / validate-curated / build-curated | `DataQualityError: ... check(s) failed: <names>` | a contract rule failed | read the `ERROR quality check` lines; inspect `outputs/quality/<layer>_checks.json` and `data/quarantine/` |
| load-postgres | `OperationalError: connection refused` | database not running / not ready | `docker compose up -d postgres`, wait for `(healthy)`, rerun (the load retries 5x itself) |
| train | `LeakageError` | a forbidden column is in the feature list, or train/test dates overlap | fix `config/settings.yml` `features.numeric` |
| reproducibility-check | `ReproducibilityError` | macro-F1 outside 0.494 ± 0.01, or split/row counts changed | check `pip freeze` vs `requirements.txt`, the `split_manifest.json` hashes, and the transform settings |
| any (Airflow) | task red | see the task log; a JSON record is also in `logs/airflow_failures.jsonl` | fix the cause, then **Clear** the failed task in the UI (it reruns it and everything downstream) |

## 3. Restart and rerun

Every step is **rerun-safe**, so the standard recovery is: *fix the cause, run again*.

- **Whole pipeline:** `python -m src.cli run-all` (or trigger the DAG again).
- **From one step onward:** run the steps individually in order, e.g.
  `python -m src.cli build-curated`, then `load-postgres`, `validate-curated`, ...
  In Airflow: click the failed task -> **Clear** (downstream tasks are cleared too).
- **Why reruns are safe:** ingestion skips committed batches (watermark); staging and curated
  are rebuilt deterministically; the PostgreSQL load is an `ON CONFLICT` upsert in one
  transaction; the run summary is an upsert by run id.

## 4. Checkpoint recovery (ingestion)

State lives in `state/checkpoint.json`: the watermark (latest committed week) and one
record per committed yearly batch (rows, file SHA-256, run id).

- **Interrupted run:** just rerun `python -m src.cli ingest`. Committed years are skipped and
  ingestion continues from the watermark (demonstrated in `docs/evidence/ingestion_recovery_demo.txt`).
- **Crash after a batch file was written but before the checkpoint moved:** the rerun rewrites
  that same batch file (same deterministic content) and then commits it, so there is no duplicate.
- **Suspected corrupted raw layer or checkpoint:** `python -m src.cli ingest --force`
  (deletes batches + checkpoint and rebuilds) or `--mode full` (byte-exact snapshot of all
  sources into `data/raw/full/<run_id>/` plus a forced rebuild).
- **Simulate a crash (demo):** `python -m src.cli ingest --stop-after 2`.

## 5. Database reset

```bash
# drop and recreate only the pipeline tables, then reload from curated files
docker exec dss150p-lab4-postgres psql -U dss150p -d dengue -c "DROP TABLE IF EXISTS curated_dengue_weekly, load_audit, pipeline_run_summary, bench_curated_dengue_weekly;"
python -m src.cli load-postgres          # recreates the schema (sql/schema.sql) and reloads

# full reset: delete the database volume (all PostgreSQL data)
docker compose down -v
docker compose up -d postgres
python -m src.cli load-postgres
```

The curated Parquet file is the source of truth for the table, so the database can always be
rebuilt from it.

## 6. Rollback and cleanup

- **A bad curated table never replaces a good one:** `build-curated` checks the table before
  writing it. On failure the previous file stays and the rejected copy goes to
  `data/quarantine/curated_rejected_<run_id>.csv`.
- **Roll back a code or config change:** `git log --oneline`, then `git revert <commit>` (or
  check out the previous version of `config/settings.yml`) and rerun `run-all`. The outputs
  are regenerated from the same source.
- **Clean all generated data** (sources are untouched):

```powershell
  Remove-Item -Recurse -Force data/raw/*, data/staging/*, data/curated/*, data/partitioned/*, data/quarantine/*, state/checkpoint.json -Exclude .gitkeep
  python -m src.cli run-all
```

- **Stop everything:** `docker compose down` (keeps the database) or `docker compose down -v`
  (deletes it).

## 7. Failure-injection drill

```bash
python -m src.cli run-all --inject-failure negative_cases   # expect exit code 1 at build-curated
python -m src.cli run-all                                   # recovery: all steps succeed
```

In Airflow, trigger with `inject_failure = negative_cases`, then with `none`.
Evidence: `docs/evidence/failure_injection_demo.txt` and `docs/evidence/airflow_failed_*.png`.