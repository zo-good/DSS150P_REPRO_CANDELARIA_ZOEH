"""Ingestion: bring the immutable source files into the raw layer.

  full mode         byte-exact snapshot of all source files in data/raw/full/<run_id>/,
                    plus a full rebuild of the dengue batches and weather copy
  incremental mode  only NEW yearly dengue batches are added, tracked by a checkpoint

The source files in data/source/ are only ever READ.
"""
import hashlib
import json
import os
import shutil

import pandas as pd
import yaml

from src.common import sha256_of, utc_now
from src.config import PROJECT_ROOT, SETTINGS, path_for, source_file

DENGUE_COLUMNS = ["loc", "cases", "deaths", "date", "Region"]


class SourceChecksumError(Exception):
    """A source file's bytes differ from the fingerprint in the manifest."""


class SimulatedInterruption(Exception):
    """Raised on purpose by --stop-after to demonstrate recovery."""


# ---------- source verification ----------

def load_manifest():
    with open(path_for("metadata_dir") / "source_manifest.yml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def count_rows(path):
    """Number of data rows in a CSV (lines minus the header)."""
    with open(path, "rb") as f:
        return sum(1 for _ in f) - 1


def verify_sources(logger):
    """Stop if a source file is missing or its checksum/row count differs from the manifest."""
    manifest = load_manifest()
    for entry in manifest["files"]:
        path = source_file(entry["name"])
        if not path.exists():
            raise FileNotFoundError(
                f"source file missing: data/source/{entry['name']} (see README: data acquisition)")
        actual = sha256_of(path)
        if actual != entry["sha256"]:
            raise SourceChecksumError(
                f"{entry['name']}: manifest sha256 {entry['sha256'][:12]}..., file has {actual[:12]}...")
        logger.info("source verified", file=entry["name"], sha256=actual[:12], rows=entry["rows"])
    return manifest


# ---------- full ingestion ----------

def ingest_full(run_id, logger):
    """Byte-exact copy of every source file into a run-specific raw folder, then rebuild."""
    manifest = verify_sources(logger)
    target = path_for("raw_dir") / "full" / run_id
    target.mkdir(parents=True, exist_ok=True)
    metadata = []
    for entry in manifest["files"]:
        copy = target / entry["name"]
        shutil.copyfile(source_file(entry["name"]), copy)
        checksum, rows = sha256_of(copy), count_rows(copy)
        if checksum != entry["sha256"] or rows != entry["rows"]:
            raise SourceChecksumError(f"raw copy of {entry['name']} does not match the manifest")
        metadata.append({
            "pipeline_run_id": run_id,
            "ingested_at_utc": utc_now(),
            "source_file": f"data/source/{entry['name']}",
            "raw_file": copy.relative_to(PROJECT_ROOT).as_posix(),
            "source_checksum": checksum,
            "rows": rows,
        })
        logger.info("raw snapshot written", file=entry["name"], rows=rows)
    (target / "_ingest_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    # a full run also rebuilds the incremental area from scratch
    ingest_incremental(run_id, logger, force=True)
    return target


# ---------- weather copy (used by staging) ----------

def ensure_weather_raw(logger):
    """Copy the weather files into data/raw/weather/ unless an identical copy is already there."""
    target = path_for("raw_dir") / "weather"
    target.mkdir(parents=True, exist_ok=True)
    for name in SETTINGS["source"]["weather_files"]:
        src, dst = source_file(name), target / name
        if dst.exists() and sha256_of(dst) == sha256_of(src):
            logger.info("weather raw copy up to date, skipped", file=name)
            continue
        tmp = dst.with_name(dst.name + ".tmp")
        shutil.copyfile(src, tmp)
        os.replace(tmp, dst)          # rename = all-or-nothing, no half-written file
        logger.info("weather raw copy written", file=name, rows=count_rows(dst))


# ---------- incremental dengue batches ----------

def checkpoint_path():
    return PROJECT_ROOT / SETTINGS["ingestion"]["checkpoint_file"]


def read_checkpoint():
    path = checkpoint_path()
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"watermark_date": None, "committed_batches": []}


def write_checkpoint(checkpoint):
    path = checkpoint_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def add_row_keys(df):
    """Stable identifiers for every source row (computed from content, never random)."""
    df.insert(0, "source_row_number", range(1, len(df) + 1))   # position in the source file
    df["source_row_key"] = df["loc"] + "|" + df["date"]         # business key: place + week
    joined = df[DENGUE_COLUMNS].agg("|".join, axis=1)
    df["record_hash"] = joined.map(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())
    return df


def ingest_incremental(run_id, logger, force=False, stop_after=None):
    """Add yearly dengue batches that are newer than the watermark in state/checkpoint.json."""
    verify_sources(logger)
    ensure_weather_raw(logger)
    batch_dir = path_for("raw_dir") / "dengue_batches"
    batch_dir.mkdir(parents=True, exist_ok=True)

    if force:
        for old in batch_dir.glob("*.csv"):
            old.unlink()
        checkpoint_path().unlink(missing_ok=True)
        logger.warning("force refresh: removed existing batches and checkpoint")

    checkpoint = read_checkpoint()
    watermark = pd.Timestamp(checkpoint["watermark_date"]) if checkpoint["watermark_date"] else None
    logger.info("checkpoint loaded", watermark=checkpoint["watermark_date"],
                committed=len(checkpoint["committed_batches"]))

    name = SETTINGS["source"]["dengue_file"]
    source_checksum = sha256_of(source_file(name))
    # dtype=str + keep_default_na=False keeps every value exactly as written in the file
    df = add_row_keys(pd.read_csv(source_file(name), dtype=str, keep_default_na=False))
    df["event_date"] = pd.to_datetime(df["date"], format=SETTINGS["source"]["dengue_date_format"],
                                      errors="coerce")

    # rows whose date cannot be read (the HXL tag row) cannot be placed in a batch
    bad = df[df["event_date"].isna()]
    if len(bad):
        qdir = path_for("quarantine_dir")
        qdir.mkdir(parents=True, exist_ok=True)
        bad.drop(columns="event_date").assign(
            quarantine_reason="unparseable date (not a data row)", pipeline_run_id=run_id
        ).to_csv(qdir / "ingest_unparseable_date.csv", index=False)
        logger.warning("rows quarantined at ingestion", rows=len(bad),
                       file="data/quarantine/ingest_unparseable_date.csv")
    good = df[df["event_date"].notna()]

    # hashes already committed, used to block duplicate logical records
    seen = set()
    for b in checkpoint["committed_batches"]:
        seen.update(pd.read_csv(batch_dir / b["file"], dtype=str, usecols=["record_hash"])["record_hash"])

    added = 0
    for year in sorted(good["event_date"].dt.year.unique()):
        batch = good[good["event_date"].dt.year == year].sort_values("source_row_number")
        batch_id = f"dengue_{year}"
        if watermark is not None and batch["event_date"].max() <= watermark:
            logger.info("batch already committed, skipped", batch=batch_id)
            continue

        batch = batch[~batch["record_hash"].isin(seen)].drop_duplicates("source_row_key")
        batch = batch.assign(pipeline_run_id=run_id, ingested_at_utc=utc_now(),
                             source_file=f"data/source/{name}", source_checksum=source_checksum,
                             batch_id=batch_id)
        out = batch_dir / f"{batch_id}.csv"
        tmp = out.with_name(out.name + ".tmp")
        batch.drop(columns="event_date").to_csv(tmp, index=False)
        os.replace(tmp, out)

        # validate what is on disk BEFORE moving the checkpoint forward
        written = pd.read_csv(out, dtype=str, keep_default_na=False)
        if len(written) != len(batch) or written["source_row_key"].duplicated().any():
            raise RuntimeError(f"{batch_id}: written file failed validation, checkpoint NOT advanced")

        seen.update(batch["record_hash"])
        checkpoint["committed_batches"].append({
            "batch_id": batch_id, "file": out.name, "rows": len(batch),
            "max_event_date": str(batch["event_date"].max().date()),
            "file_sha256": sha256_of(out), "pipeline_run_id": run_id, "committed_at_utc": utc_now(),
        })
        checkpoint["watermark_date"] = str(batch["event_date"].max().date())
        write_checkpoint(checkpoint)
        logger.info("batch committed", batch=batch_id, rows=len(batch),
                    watermark=checkpoint["watermark_date"])
        added += 1
        if stop_after is not None and added >= stop_after:
            raise SimulatedInterruption(f"stopped on purpose after {added} batch(es) (--stop-after)")

    total = sum(b["rows"] for b in checkpoint["committed_batches"])
    logger.info("incremental ingestion finished", batches_added=added, total_raw_rows=total,
                watermark=checkpoint["watermark_date"])
    return checkpoint