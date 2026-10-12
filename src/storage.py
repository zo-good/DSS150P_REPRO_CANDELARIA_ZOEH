"""PostgreSQL storage: create the schema and load the curated table rerun-safely.

Rerun safety: rows go into a temporary table first, then
  INSERT ... ON CONFLICT (loc, date) DO UPDATE ... WHERE the row really changed.
Everything happens in ONE transaction, so a crash leaves the table as it was.
"""
import hashlib
import time

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from src.config import PROJECT_ROOT, db_settings, path_for

TABLE = "curated_dengue_weekly"
KEY = ["loc", "date"]
NOT_HASHED = {"pipeline_run_id", "processed_at_utc", "row_hash", "loaded_at_utc"}
HASH_DECIMALS = 6   # floats are rounded before hashing: Windows/Linux numpy can differ ~1e-14


def connect(retries=5, wait_seconds=3):
    """Connect to PostgreSQL, retrying while the database container starts up."""
    settings = db_settings()
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(**settings, connect_timeout=5)
        except psycopg2.OperationalError:
            if attempt == retries:
                raise
            time.sleep(wait_seconds)


def ensure_schema(conn):
    """Run sql/schema.sql (CREATE TABLE IF NOT EXISTS, so it is safe to repeat)."""
    with conn.cursor() as cur:
        cur.execute((PROJECT_ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
    conn.commit()


def table_columns(conn, table):
    """Column names of a table, in table order, except the DB-filled loaded_at_utc."""
    with conn.cursor() as cur:
        cur.execute("SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position", (table,))
        return [r[0] for r in cur.fetchall() if r[0] != "loaded_at_utc"]


def prepare_rows(curated, columns):
    """Rename/convert curated columns to match the table and add a row hash."""
    df = curated.rename(columns={"Region": "region"}).copy()
    df["deaths"] = df["deaths"].astype("int64")
    for col in ["date", "week_start", "week_end"]:
        df[col] = pd.to_datetime(df[col]).dt.date
    hashed = df[[c for c in columns if c not in NOT_HASHED]].copy()
    floats = hashed.select_dtypes("float").columns
    hashed[floats] = hashed[floats].round(HASH_DECIMALS)
    joined = hashed.astype(object).fillna("NULL").astype(str).agg("|".join, axis=1)
    df["row_hash"] = joined.map(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())
    df = df[columns].astype(object).where(df[columns].notna(), None)   # NaN -> SQL NULL
    return df


def load_curated(run_id, logger):
    """Upsert data/curated/dengue_weather_weekly.parquet into PostgreSQL."""
    curated = pd.read_parquet(path_for("curated_dir") / "dengue_weather_weekly.parquet")
    conn = connect()
    try:
        ensure_schema(conn)
        columns = table_columns(conn, TABLE)
        rows = prepare_rows(curated, columns)
        col_list = ", ".join(columns)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in KEY)
        with conn.cursor() as cur:
            cur.execute(f"CREATE TEMP TABLE tmp_load (LIKE {TABLE} INCLUDING DEFAULTS) ON COMMIT DROP")
            execute_values(cur, f"INSERT INTO tmp_load ({col_list}) VALUES %s",
                           rows.itertuples(index=False, name=None), page_size=2000)
            cur.execute(f"""
                INSERT INTO {TABLE} ({col_list})
                SELECT {col_list} FROM tmp_load
                ON CONFLICT (loc, date) DO UPDATE SET {updates}, loaded_at_utc = now()
                WHERE {TABLE}.row_hash IS DISTINCT FROM EXCLUDED.row_hash
                RETURNING (xmax = 0) AS inserted
            """)
            results = [r[0] for r in cur.fetchall()]
            inserted = sum(results)
            updated = len(results) - inserted
            unchanged = len(rows) - len(results)
            cur.execute(f"SELECT count(*) FROM {TABLE}")
            in_table = cur.fetchone()[0]
            cur.execute("""INSERT INTO load_audit (pipeline_run_id, table_name, rows_in_file,
                           rows_inserted, rows_updated, rows_unchanged, rows_in_table)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (run_id, TABLE, len(rows), inserted, updated, unchanged, in_table))
        conn.commit()                      # all-or-nothing
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info("postgres load committed", table=TABLE, rows_in_file=len(rows), inserted=inserted,
                updated=updated, unchanged=unchanged, rows_in_table=in_table)
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged, "rows_in_table": in_table}