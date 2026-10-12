"""Airflow DAG for the DSS150P Lab 4 dengue reproduction pipeline.

This file only ORCHESTRATES. Every task runs one step of the project CLI
(`python -m src.cli <step>`) in the pipeline's own virtual environment, so no
ingestion, transformation or modelling logic lives here.
"""
import json
import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, chain

PROJECT_DIR = os.environ.get("PIPELINE_PROJECT_DIR", "/opt/project")
PIPELINE_PYTHON = os.environ.get("PIPELINE_PYTHON", "/opt/pipeline-venv/bin/python")

# One pipeline_run_id per DAG run, shared by every task so all outputs trace back to it.
# Characters like ':' and '+' are removed because the id is also used in file names.
RUN_ID = "af_{{ dag_run.run_id | replace(':', '') | replace('+', '') | replace('.', '') }}"


def record_failure(context):
    """Failure callback: one JSON line per failed task in logs/airflow_failures.jsonl."""
    ti = context["task_instance"]
    entry = {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "airflow_run_id": context["run_id"],
        "try_number": ti.try_number,
        "error": str(context.get("exception")),
    }
    os.makedirs(os.path.join(PROJECT_DIR, "logs"), exist_ok=True)
    with open(os.path.join(PROJECT_DIR, "logs", "airflow_failures.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def step(task_id, cli_args, retries=0, timeout_minutes=15):
    """One pipeline step = one task running one CLI command."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && {PIPELINE_PYTHON} -m src.cli --run-id {RUN_ID} {cli_args}",
        retries=retries,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=timeout_minutes),
        on_failure_callback=record_failure,
    )


with DAG(
    dag_id="predictive_repro_pipeline",
    description="Reproducible dengue severity pipeline: source -> raw -> staging -> curated -> model",
    schedule=None,        # manual trigger: the source is a fixed snapshot, nothing arrives on a timetable
    start_date=datetime(2026, 1, 1),
    catchup=False,        # never back-fill old dates
    max_active_runs=1,    # two runs at once would race on the same checkpoint and files
    params={
        "mode": Param("incremental", enum=["incremental", "full"],
                      description="incremental = only new yearly batches; full = rebuild raw from source"),
        "inject_failure": Param("none", enum=["none", "negative_cases", "duplicate_key", "missing_column"],
                                description="failure-injection demo: corrupt this run's curated table"),
    },
    tags=["dss150p", "lab4"],
) as dag:
    # retries only where a retry is safe: checkpointed ingestion, transactional upsert, upserted summary
    ingest_source = step("ingest_source", "ingest --mode {{ params.mode }}", retries=2)
    validate_raw = step("validate_raw", "validate-raw")               # bad data will not fix itself
    transform_staging = step("transform_staging", "transform-staging", retries=1)
    build_curated = step(
        "build_curated",
        "build-curated{% if params.inject_failure != 'none' %} --inject-failure {{ params.inject_failure }}{% endif %}")
    load_postgres = step("load_postgres", "load-postgres", retries=3)
    validate_curated = step("validate_curated", "validate-curated")
    train_model = step("train_model", "train", timeout_minutes=20)
    evaluate_model = step("evaluate_model", "evaluate")
    reproducibility_check = step("reproducibility_check", "reproducibility-check")
    publish_outputs = step("publish_outputs", "publish-outputs", retries=2)

    chain(ingest_source, validate_raw, transform_staging, build_curated, load_postgres,
          validate_curated, train_model, evaluate_model, reproducibility_check, publish_outputs)