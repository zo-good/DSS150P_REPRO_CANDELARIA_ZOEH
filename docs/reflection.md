# Analysis and Reflection (Lab 4, section 14)

**1. What aspects of the previous project prevented another person from reproducing it immediately?**
Paths like `../data/processed/` only worked from inside `notebooks/`; 14 notebooks had to be run
by hand in the right order; one step (the weekly weather file) had no code at all; there was no
dependency file, so library versions were unknown; the row order that the random forest depends
on was never fixed; and the metrics existed only as notebook printouts, which even disagreed
between notebooks 12 and 14. Two silent data problems (a dropped city, duplicated weather rows)
were also invisible without checks.

**2. Which artifact is the true authoritative source: the original file, the raw copy, the curated
dataset, or the model-ready dataset? Explain the role of each.**
The **original file** in `data/source/` is the authority: it is never edited and is pinned by
SHA-256. The **raw copy** is evidence of exactly what entered the pipeline and when (audit
columns, run id). The **curated dataset** is the trusted data product for consumers (the
PostgreSQL table, analysts, the model). The **model-ready dataset** (split + severity labels) is
derived per run and is not authoritative: it is regenerated from curated every time, because its
labels depend on the split.

**3. What identifier or rule did you use to prevent duplicates, and what assumption makes that rule
valid?**
The business key `(loc, date)`: one location per epidemiological week. It appears as
`source_row_key` in raw, as the duplicate check in staging, and as the PostgreSQL primary key,
plus a `record_hash` of the row's content. The assumption is that DOH reports exactly one row per
location per week, and a correction would be an update to that row, not a second row. The same
assumption is **false** for the weather file (4 cities have two rows per day), which is why that
case is detected and handled by an explicit policy instead.

**4. If your dataset has no natural update timestamp, how did you simulate incremental ingestion
without pretending the generated checkpoint is a real business timestamp?**
The dengue file does have a real event date (the week-end Sunday), so batches are calendar years
of that date and the watermark is the latest committed week. But the file is a static snapshot,
so the *arrival* of yearly batches is simulated: the watermark records "latest event date already
ingested", not "when DOH published it". This is stated in the code and docs as a laboratory
simulation.

**5. Which transformations belong in staging versus curated, and why?**
Staging is per source and changes no business meaning: type conversion, date parsing, contract
rules, quarantining bad rows, detecting duplicates. Curated applies business logic across
sources: weekly weather aggregation, the city mapping join, the `deaths -> 0` rule, and feature
engineering. The split and severity labels belong to model input (`train.py`), not curated,
because the thresholds must be computed from the training rows only.

**6. Which storage format performed best for your workload? Why should your result not be generalized?**
Parquet for pipeline storage and model input (1.9 MB vs 7.4 MB CSV; about 10 ms reads vs about
100 ms), PostgreSQL for selective questions (about 3 ms for one city-year thanks to the primary
key index), and CSV for exchange. This should not be generalized: the table is small (10k rows),
flat, and measured on one laptop with a warm file cache and a local Docker database. A large,
wide, nested or concurrently-used dataset could rank the formats differently.

**7. What happens if Airflow retries your load task after the database committed but the task process
did not receive confirmation?**
The retry runs the same upsert again. Every `(loc, date)` already exists with the same row hash,
so the database inserts 0 rows and updates 0 rows; only one extra `load_audit` row records the
retry. The table content does not change.

**8. How does your design make that retry safe?**
(1) The load is one transaction: either everything commits or nothing does. (2) The primary key
makes a duplicate row impossible. (3) `INSERT ... ON CONFLICT (loc, date) DO UPDATE ... WHERE
row_hash IS DISTINCT FROM EXCLUDED.row_hash` updates only rows that really changed. (4) The input
file is deterministic. Ingestion is safe in the same way (checkpoint advanced only after the batch
is written and re-read), and the run summary is an upsert by run id.

**9. Which data-quality check is most likely to detect a change that would silently damage model accuracy?**
`cases_lag1_equals_previous_week`: it confirms every lag feature really is last week's value for
the same city. A wrong sort order, a missing week or a shifted join would silently scramble the
features (or leak the future) without any error, and this check catches it; it also caught the
injected bad row. A close second is `expected_city_count`, which would have exposed the group's
silent Cagayan de Oro drop.

**10. How do you know that your training and test rows are the same logical rows used in the baseline?**
The curated table matches the group's model-ready file on all 10,433 `(loc, date)` keys and all
49 compared columns; the split date (2020-01-12) and counts (8,373 / 2,060) are identical; the
naive baseline, which uses only data and no model, matches (MAE 3.8307 vs 3.83); and
`split_manifest.json` stores SHA-256 fingerprints of the train keys, test keys and row order.

**11. What sources of nondeterminism remain in your model or environment?**
Seed sensitivity (macro-F1 sd 0.0057 across seeds 0-19), so the tolerance only holds for the
declared seed 42; library versions (results differ between scikit-learn releases); the group-order
sort relies on pandas' default sort algorithm (protected by the order hash); and tiny
floating-point differences between Windows and Linux (about 1e-14), which the row hash tolerates
by rounding.

**12. If the reproduced metric differs from the original, how can you determine whether the cause is
data, preprocessing, split logic, dependencies, model parameters, or nondeterministic execution?**
Check one layer at a time, from the source outward: source checksums (data) -> curated vs baseline
rows and columns (preprocessing) -> split date, counts and key hashes (split logic) -> the naive
baseline (data and target, independent of the model) -> `pip freeze` vs `requirements.txt`
(dependencies) -> settings vs `expected_metrics.json` (parameters) -> run twice in the same
environment, then vary the seed (nondeterminism). This is the order used in this project: it is
how the row-order effect and the scikit-learn version effect were found.

**13. What would you monitor if this pipeline ran every day in production?**
Source arrival and checksum changes; row counts per layer against recent runs; quarantine rate;
quality-check pass/fail; null rates and the city count; drift in the distributions of cases and
weather; run duration, retries and failures (from `logs/pipeline.jsonl` and
`pipeline_run_summary`); `load_audit` insert/update counts; and the model's macro-F1 against the
naive baseline over time.

**14. What part of your current architecture would become the first bottleneck if the dataset grew by 100x?**
The in-memory pandas rebuild of staging: every run re-reads all raw batches and about 700k weather
rows into memory (70 million at 100x) and rebuilds staging and curated from scratch. I would make
staging incremental per partition (only new years), use a columnar engine (DuckDB or Spark), and
load PostgreSQL with `COPY` into partitioned tables. The single-container LocalExecutor Airflow
would be next.

**15. What would you change if the dataset became sensitive or regulated data?**
Health data is sensitive personal information under the Philippine Data Privacy Act of 2012
(RA 10173). I would stop committing any data (manifest and checksum only), keep the files in
access-controlled, encrypted storage, move credentials to a secrets manager, pseudonymize or
aggregate identifiers and suppress small counts, remove data values from logs, and protect the
quarantine folder too, because it contains copies of real rows. I would also add retention rules
and an access audit trail.