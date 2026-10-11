"""Shared helpers: run IDs, UTC timestamps, file hashing, structured logging."""
import hashlib
import json
import uuid
from datetime import datetime, timezone

from src.config import path_for


def utc_now():
    """Current time in UTC as an ISO string, e.g. 2026-10-11T15:30:00+00:00."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_run_id():
    """Unique ID for one pipeline run, e.g. run_20261011T153000Z_a1b2c3."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{stamp}_{uuid.uuid4().hex[:6]}"


def sha256_of(path):
    """Fingerprint of a file's exact bytes, read in 1 MB chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class RunLogger:
    """Structured logger for one pipeline stage of one run.

    Every message is printed to the terminal AND appended to logs/pipeline.jsonl
    as one JSON object per line: timestamp, level, stage, run id, message,
    plus any extra fields you pass (row counts, durations, errors...).
    """

    def __init__(self, stage, run_id):
        self.stage = stage
        self.run_id = run_id
        self.log_file = path_for("logs_dir") / "pipeline.jsonl"
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def _write(self, level, message, fields):
        entry = {"ts_utc": utc_now(), "level": level, "stage": self.stage,
                 "pipeline_run_id": self.run_id, "message": message, **fields}
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
        extras = " ".join(f"{k}={v}" for k, v in fields.items())
        print(f"{level:<7} [{self.stage}] {message} {extras}".rstrip())

    def info(self, message, **fields):
        self._write("INFO", message, fields)

    def warning(self, message, **fields):
        self._write("WARNING", message, fields)

    def error(self, message, **fields):
        self._write("ERROR", message, fields)