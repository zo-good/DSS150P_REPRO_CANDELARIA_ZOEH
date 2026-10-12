"""Publish: one run-summary record per pipeline run (file + PostgreSQL).

Collects row counts for every layer, the headline metric, and the start/end/status of
every step from logs/pipeline.jsonl, for ONE pipeline_run_id.
"""
import json

import pandas as pd
import pyarrow.parquet as pq
from psycopg2.extras import Json

from src.common import utc_now
from src.config import PROJECT_ROOT, SETTINGS, path_for
from src.storage import connect, ensure_schema


def step_history(run_id):
    """Start time, end time and final status of every step logged under this run id."""
    steps = {}
    log_file = path_for("logs_dir") / "pipeline.jsonl"
    for line in log_file.read_text(encoding="utf-8").splitlines():
        e = json.loads(line)
        if e.get("pipeline_run_id") != run_id:
            continue
        s = steps.setdefault(e["stage"], {"started_at_utc": None, "finished_at_utc": None,
                                          "status": "running"})
        if e["message"] == "step started":
            s["started_at_utc"] = e["ts_utc"]
        elif e["message"] == "step finished":
            s.update(finished_at_utc=e["ts_utc"], status="success", duration_s=e.get("duration_s"))
        elif e["message"] == "step failed":
            s.update(finished_at_utc=e["ts_utc"], status="failed", error=e.get("error"))
    return steps


def count_csv_rows(folder, pattern):
    return int(sum(len(pd.read_csv(f)) for f in folder.glob(pattern)))


def run_publish(run_id, logger):
    checkpoint = json.loads((PROJECT_ROOT / SETTINGS["ingestion"]["checkpoint_file"]).read_text())
    split = json.loads((path_for("metadata_dir") / "split_manifest.json").read_text())
    metrics = json.loads((path_for("outputs_dir") / "metrics.json").read_text())
    comparison = json.loads((path_for("outputs_dir") / "metrics_comparison.json").read_text())
    steps = step_history(run_id)
    steps["publish-outputs"] = {"started_at_utc": utc_now(), "status": "running"}
    counts = {
        "raw_rows": sum(b["rows"] for b in checkpoint["committed_batches"]),
        "quarantined_rows": (count_csv_rows(path_for("quarantine_dir"), "ingest_*.csv")
                             + count_csv_rows(path_for("quarantine_dir"), "staging_*.csv")),
        "staging_rows": pq.ParquetFile(path_for("staging_dir") / "dengue_weekly.parquet").metadata.num_rows,
        "curated_rows": split["rows"]["curated"],
        "train_rows": split["rows"]["train"],
        "test_rows": split["rows"]["test"],
        "prediction_rows": len(pd.read_csv(path_for("outputs_dir") / "predictions.csv")),
    }
    failed = [name for name, s in steps.items() if s["status"] == "failed"]
    status = "failed" if failed or not comparison["passed"] else "success"
    started = min((s["started_at_utc"] for s in steps.values() if s.get("started_at_utc")), default=None)
    summary = {"pipeline_run_id": run_id, "status": status, "started_at_utc": started,
               "finished_at_utc": utc_now(), **counts,
               "primary_metric": comparison["primary_metric"],
               "primary_metric_value": comparison["actual"],
               "expected": comparison["expected"], "tolerance": comparison["tolerance"],
               "reproducibility_passed": comparison["passed"],
               "metrics": {k: v for k, v in metrics.items() if k not in ("pipeline_run_id", "computed_at_utc")},
               "steps": steps}
    (path_for("outputs_dir") / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    cols = ["pipeline_run_id", "status", "started_at_utc", "finished_at_utc", *counts,
            "primary_metric", "primary_metric_value", "reproducibility_passed"]
    conn = connect()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO pipeline_run_summary ({', '.join(cols)}, summary)
                VALUES ({', '.join(['%s'] * len(cols))}, %s)
                ON CONFLICT (pipeline_run_id) DO UPDATE SET
                {', '.join(f'{c} = EXCLUDED.{c}' for c in cols[1:])}, summary = EXCLUDED.summary,
                published_at_utc = now()""",
                [summary[c] for c in cols] + [Json(summary)])
        conn.commit()
    finally:
        conn.close()
    logger.info("run summary published", status=status, file="outputs/run_summary.json",
                table="pipeline_run_summary", **counts)
    return summary