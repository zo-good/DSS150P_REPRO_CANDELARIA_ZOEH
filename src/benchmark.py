"""Storage benchmark: the SAME curated records stored as CSV, JSONL, Parquet and PostgreSQL.

For each format: size on disk, write time, read time (1 cold read + 5 warm reads, median
of the warm reads), and a check that the records read back are the same logical records.
Then: year partitions in Parquet, and reading ONE partition on its own.
Results -> docs/benchmark_results.json and docs/benchmark_report.md
"""
import hashlib
import io
import json
import shutil
import statistics
import time

import pandas as pd

from src.common import utc_now
from src.config import PROJECT_ROOT, path_for
from src.storage import connect, ensure_schema

READ_REPEATS = 5
BENCH_TABLE = "bench_curated_dengue_weekly"
FILTER = {"loc": "QUEZON CITY", "start": "2019-01-01", "end": "2019-12-31"}


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - start


def fingerprint(df):
    """Same logical records? Hash of the sorted (loc, date, cases) keys + values."""
    d = df[["loc", "date", "cases"]].copy()
    d["date"] = pd.to_datetime(d["date"]).dt.strftime("%Y-%m-%d")
    d = d.sort_values(["loc", "date"], kind="mergesort")
    text = "\n".join(d["loc"] + "|" + d["date"] + "|" + d["cases"].astype(str))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_times(read_fn):
    """1 cold read (reported separately) + READ_REPEATS warm reads (median reported)."""
    df, cold = timed(read_fn)
    warm = [timed(read_fn)[1] for _ in range(READ_REPEATS)]
    return df, cold, statistics.median(warm)


# ---------------- file formats ----------------

def file_formats(df, folder):
    return {
        "csv": (lambda: df.to_csv(folder / "curated.csv", index=False),
                lambda: pd.read_csv(folder / "curated.csv", parse_dates=["date"]),
                folder / "curated.csv"),
        "jsonl": (lambda: df.to_json(folder / "curated.jsonl", orient="records", lines=True,
                                     date_format="iso"),
                  lambda: pd.read_json(folder / "curated.jsonl", lines=True, convert_dates=["date"]),
                  folder / "curated.jsonl"),
        "parquet": (lambda: df.to_parquet(folder / "curated.parquet", index=False),
                    lambda: pd.read_parquet(folder / "curated.parquet"),
                    folder / "curated.parquet"),
    }


# ---------------- PostgreSQL ----------------

def pg_write(conn, df):
    """Fresh copy of the records in a benchmark table, loaded with COPY."""
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {BENCH_TABLE}")
        cur.execute(f"CREATE TABLE {BENCH_TABLE} (LIKE curated_dengue_weekly INCLUDING ALL)")
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = "
                    f"'{BENCH_TABLE}' AND column_name NOT IN ('loaded_at_utc') ORDER BY ordinal_position")
        cols = [r[0] for r in cur.fetchall()]
        out = df.rename(columns={"Region": "region"}).copy()
        out["deaths"] = out["deaths"].astype("int64")
        out["row_hash"] = "benchmark"
        buf = io.StringIO()
        out[cols].to_csv(buf, index=False, header=False)
        buf.seek(0)
        cur.copy_expert(f"COPY {BENCH_TABLE} ({', '.join(cols)}) FROM STDIN WITH CSV", buf)
    conn.commit()


def pg_query(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        names = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=names)


def pg_size(conn):
    return pg_query(conn, f"SELECT pg_total_relation_size('{BENCH_TABLE}') AS b")["b"].iloc[0]


# ---------------- partitions ----------------

def partition_by_year(df, root):
    """Write one Parquet folder per year: data/partitioned/dengue_weather_weekly/year=YYYY/."""
    if root.exists():
        shutil.rmtree(root)          # rebuild from scratch so reruns never duplicate files
    df = df.assign(year=df["date"].dt.year)
    df.to_parquet(root, partition_cols=["year"], index=False)
    return sorted(p.name for p in root.iterdir() if p.is_dir())


# ---------------- main ----------------

def run_benchmark(run_id, logger):
    curated = pd.read_parquet(path_for("curated_dir") / "dengue_weather_weekly.parquet")
    expected_fp, n = fingerprint(curated), len(curated)
    folder = path_for("curated_dir") / "formats"
    folder.mkdir(parents=True, exist_ok=True)
    results = []

    for name, (write, read, path) in file_formats(curated, folder).items():
        _, write_s = timed(write)
        back, cold_s, warm_s = read_times(read)
        results.append({"format": name, "size_bytes": path.stat().st_size, "write_s": write_s,
                        "read_cold_s": cold_s, "read_warm_median_s": warm_s, "rows_read": len(back),
                        "same_logical_records": fingerprint(back) == expected_fp})
        logger.info("format benchmarked", format=name, size_bytes=path.stat().st_size,
                    write_s=round(write_s, 3), read_warm_median_s=round(warm_s, 3))

    conn = connect()
    try:
        ensure_schema(conn)
        _, write_s = timed(lambda: pg_write(conn, curated))
        full_sql = f"SELECT * FROM {BENCH_TABLE}"
        back, cold_s, warm_s = read_times(lambda: pg_query(conn, full_sql))
        filt_sql = f"SELECT * FROM {BENCH_TABLE} WHERE loc = %s AND date BETWEEN %s AND %s"
        params = (FILTER["loc"], FILTER["start"], FILTER["end"])
        filt, f_cold, f_warm = read_times(lambda: pg_query(conn, filt_sql, params))
        results.append({"format": "postgresql", "size_bytes": int(pg_size(conn)), "write_s": write_s,
                        "read_cold_s": cold_s, "read_warm_median_s": warm_s, "rows_read": len(back),
                        "same_logical_records": fingerprint(back) == expected_fp})
        pg_filter = {"query": filt_sql, "params": list(params), "rows": len(filt),
                     "read_cold_s": f_cold, "read_warm_median_s": f_warm}
        logger.info("postgresql benchmarked", write_s=round(write_s, 3),
                    full_read_warm_median_s=round(warm_s, 3), filter_rows=len(filt),
                    filter_read_warm_median_s=round(f_warm, 4))
    finally:
        conn.close()

    # file formats answering the same selective question (read everything, then filter)
    mask = lambda d: (d["loc"] == FILTER["loc"]) & (pd.to_datetime(d["date"]).between(FILTER["start"], FILTER["end"]))
    file_filter = {}
    for name, (_, read, _) in file_formats(curated, folder).items():
        _, _, warm = read_times(lambda: read().loc[mask])
        file_filter[name] = warm

    # partitions
    root = path_for("partitioned_dir") / "dengue_weather_weekly"
    parts = partition_by_year(curated, root)
    one, p_cold, p_warm = read_times(lambda: pd.read_parquet(root / "year=2019"))
    expected_2019 = int((curated["date"].dt.year == 2019).sum())
    partition = {"partition_column": "year (from date)", "partitions": parts,
                 "selected": "year=2019", "rows_read": len(one), "rows_expected": expected_2019,
                 "row_count_ok": len(one) == expected_2019,
                 "read_cold_s": p_cold, "read_warm_median_s": p_warm,
                 "full_parquet_then_filter_warm_median_s":
                     read_times(lambda: pd.read_parquet(folder / "curated.parquet")
                                .loc[lambda d: d["date"].dt.year == 2019])[2]}
    logger.info("partition read", partition="year=2019", rows=len(one), expected=expected_2019,
                ok=partition["row_count_ok"])

    report = {"pipeline_run_id": run_id, "created_at_utc": utc_now(), "records": n,
              "read_repeats": READ_REPEATS,
              "timing_note": "read_cold_s = first read; read_warm_median_s = median of the next "
                             f"{READ_REPEATS} reads (cold run NOT included in the median)",
              "formats": results, "postgresql_filter_query": pg_filter,
              "file_filter_warm_median_s": file_filter, "partition_demo": partition}
    docs = PROJECT_ROOT / "docs"
    (docs / "benchmark_results.json").write_text(json.dumps(report, indent=2, default=float),
                                                 encoding="utf-8")
    write_markdown(report, docs / "benchmark_report.md")
    if not all(r["same_logical_records"] for r in results) or not partition["row_count_ok"]:
        raise RuntimeError("benchmark formats do not contain the same logical records")
    return report


def write_markdown(r, path):
    ms = lambda s: f"{s * 1000:,.1f} ms"
    lines = ["# Storage Benchmark Report", "",
             f"Generated by `python -m src.cli benchmark` (run `{r['pipeline_run_id']}`, "
             f"{r['created_at_utc']}). Same machine, same {r['records']:,} curated records in every format.",
             "", f"Timing: {r['timing_note']}.", "",
             "## Size, write and read", "",
             "| Format | Size | Write | Read (cold) | Read (warm median) | Rows read | Same records |",
             "|---|---:|---:|---:|---:|---:|:---:|"]
    for f in r["formats"]:
        lines.append(f"| {f['format']} | {f['size_bytes'] / 1e6:.2f} MB | {ms(f['write_s'])} | "
                     f"{ms(f['read_cold_s'])} | {ms(f['read_warm_median_s'])} | {f['rows_read']:,} | "
                     f"{'yes' if f['same_logical_records'] else 'NO'} |")
    q = r["postgresql_filter_query"]
    lines += ["", "PostgreSQL size = table + indexes (`pg_total_relation_size`).", "",
              "## Selective question: one city, one year", "",
              f"`{q['query']}` with {q['params']} -> {q['rows']} rows.", "",
              "| Approach | Warm median |", "|---|---:|",
              f"| PostgreSQL `WHERE` query | {ms(q['read_warm_median_s'])} |"]
    for name, s in r["file_filter_warm_median_s"].items():
        lines.append(f"| {name}: read whole file, then filter in pandas | {ms(s)} |")
    p = r["partition_demo"]
    lines += ["", "## Partitioning", "",
              f"- Partition column: **{p['partition_column']}**: {', '.join(p['partitions'])}",
              f"- Read only `{p['selected']}`: {p['rows_read']:,} rows (expected {p['rows_expected']:,}) -> "
              f"{'OK' if p['row_count_ok'] else 'MISMATCH'}",
              f"- One partition: {ms(p['read_warm_median_s'])} vs whole Parquet file + filter: "
              f"{ms(p['full_parquet_then_filter_warm_median_s'])}", ""]
    notes = PROJECT_ROOT / "docs" / "benchmark_notes.md"
    if notes.exists():
        lines.append(notes.read_text(encoding="utf-8"))
    path.write_text("\n".join(lines), encoding="utf-8")