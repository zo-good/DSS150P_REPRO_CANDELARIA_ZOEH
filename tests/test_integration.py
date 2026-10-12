"""Integration tests: several modules together, crossing file (and database) boundaries.

They run the real pipeline code on a temporary COPY of the project (see conftest.py).
"""
import json

import pandas as pd
import pytest

from src import ingest, transform, validate
from src.config import path_for


@pytest.fixture(scope="module")
def ingested(sandbox_project):
    """Interrupted ingestion, then resume, exactly like the recovery demo."""
    from tests.conftest import ListLogger
    log = ListLogger()
    with pytest.raises(ingest.SimulatedInterruption):
        ingest.ingest_incremental("test_run_1", log, stop_after=2)
    ingest.ingest_incremental("test_run_2", log)
    return sandbox_project


def raw_batches():
    files = sorted((path_for("raw_dir") / "dengue_batches").glob("*.csv"))
    return pd.concat([pd.read_csv(f, dtype=str) for f in files], ignore_index=True)


def test_ingestion_resume_and_rerun_create_no_duplicates(ingested, logger):
    before = raw_batches()
    checkpoint = ingest.ingest_incremental("test_run_3", logger)        # nothing new
    after = raw_batches()
    assert len(before) == len(after) == 32701                           # 32,702 source rows - 1 HXL row
    assert not after["source_row_key"].duplicated().any()
    assert checkpoint["watermark_date"] == "2021-01-10"
    assert len(checkpoint["committed_batches"]) == 6
    assert any(m == "incremental ingestion finished" and f["batches_added"] == 0
               for _, m, f in logger.messages)


def test_raw_to_curated_reproduces_group_dataset(ingested, logger):
    transform.run_staging("test_run_4", logger)
    curated = transform.run_curated("test_run_4", logger)
    assert len(curated) == 10433                                        # group's model-ready rows
    assert curated["loc"].nunique() == 41
    assert all(r["passed"] for r in validate.curated_checks(curated))


def test_quality_gate_blocks_bad_data_and_keeps_good_file(ingested, logger):
    good = path_for("curated_dir") / "dengue_weather_weekly.parquet"
    if not good.exists():
        transform.run_staging("test_run_5", logger)
        transform.run_curated("test_run_5", logger)
    stamp = good.stat().st_mtime_ns
    for injection in transform.INJECTIONS:
        with pytest.raises(validate.DataQualityError):
            transform.run_curated(f"test_inject_{injection}", logger, inject=injection)
    assert good.stat().st_mtime_ns == stamp                             # good file untouched


def test_postgres_load_is_rerun_safe(ingested, logger):
    from src import storage
    try:
        storage.connect(retries=1).close()
    except Exception:
        pytest.skip("PostgreSQL not reachable (start it with: docker compose up -d postgres)")
    if not (path_for("curated_dir") / "dengue_weather_weekly.parquet").exists():
        transform.run_staging("test_run_6", logger)
        transform.run_curated("test_run_6", logger)
    storage.load_curated("test_load_1", logger)
    second = storage.load_curated("test_load_2", logger)
    assert second["inserted"] == 0 and second["updated"] == 0
    assert second["rows_in_table"] == 10433