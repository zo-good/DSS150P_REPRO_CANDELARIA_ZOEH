-- DSS150P Lab 4 - PostgreSQL schema (safe to run many times: IF NOT EXISTS)

-- Curated, model-ready data product: one row per city per epidemiological week.
-- (loc, date) is the business key; the primary key makes duplicates impossible.
CREATE TABLE IF NOT EXISTS curated_dengue_weekly (
    loc                            TEXT             NOT NULL,
    date                           DATE             NOT NULL,   -- week end (Sunday)
    region                         TEXT             NOT NULL,
    cases                          INTEGER          NOT NULL CHECK (cases >= 0),
    deaths                         INTEGER          NOT NULL CHECK (deaths >= 0),
    week_start                     DATE             NOT NULL,
    week_end                       DATE             NOT NULL,
    precipitation_sum              DOUBLE PRECISION,
    rain_sum                       DOUBLE PRECISION,
    precipitation_hours            DOUBLE PRECISION,
    sunshine_duration              DOUBLE PRECISION,
    daylight_duration              DOUBLE PRECISION,
    shortwave_radiation_sum        DOUBLE PRECISION,
    et0_fao_evapotranspiration     DOUBLE PRECISION,
    temperature_2m_max             DOUBLE PRECISION,
    temperature_2m_min             DOUBLE PRECISION,
    temperature_2m_mean            DOUBLE PRECISION,
    apparent_temperature_max       DOUBLE PRECISION,
    apparent_temperature_min       DOUBLE PRECISION,
    apparent_temperature_mean      DOUBLE PRECISION,
    wind_speed_10m_max             DOUBLE PRECISION,
    wind_gusts_10m_max             DOUBLE PRECISION,
    wind_direction_10m_dominant    DOUBLE PRECISION,
    week_of_year                   INTEGER,
    month                          INTEGER,
    woy_sin                        DOUBLE PRECISION,
    woy_cos                        DOUBLE PRECISION,
    rain_sum_lag1                  DOUBLE PRECISION,
    rain_sum_lag2                  DOUBLE PRECISION,
    rain_sum_lag3                  DOUBLE PRECISION,
    rain_sum_lag4                  DOUBLE PRECISION,
    precipitation_sum_lag1         DOUBLE PRECISION,
    precipitation_sum_lag2         DOUBLE PRECISION,
    precipitation_sum_lag3         DOUBLE PRECISION,
    precipitation_sum_lag4         DOUBLE PRECISION,
    temperature_2m_mean_lag1       DOUBLE PRECISION,
    temperature_2m_mean_lag2       DOUBLE PRECISION,
    temperature_2m_mean_lag3       DOUBLE PRECISION,
    temperature_2m_mean_lag4       DOUBLE PRECISION,
    temperature_2m_max_lag1        DOUBLE PRECISION,
    temperature_2m_max_lag2        DOUBLE PRECISION,
    temperature_2m_max_lag3        DOUBLE PRECISION,
    temperature_2m_max_lag4        DOUBLE PRECISION,
    apparent_temperature_mean_lag1 DOUBLE PRECISION,
    apparent_temperature_mean_lag2 DOUBLE PRECISION,
    apparent_temperature_mean_lag3 DOUBLE PRECISION,
    apparent_temperature_mean_lag4 DOUBLE PRECISION,
    rain_sum_roll4                 DOUBLE PRECISION,
    temperature_2m_mean_roll4      DOUBLE PRECISION,
    cases_lag1                     DOUBLE PRECISION,
    cases_lag2                     DOUBLE PRECISION,
    cases_lag3                     DOUBLE PRECISION,
    cases_next                     DOUBLE PRECISION,          -- NULL for each city's last week
    log_cases                      DOUBLE PRECISION,
    -- audit columns
    source_row_key                 TEXT             NOT NULL,   -- loc|date from the raw file
    record_hash                    TEXT             NOT NULL,   -- hash of the raw source row
    row_hash                       TEXT             NOT NULL,   -- hash of this curated row's values
    pipeline_run_id                TEXT             NOT NULL,
    processed_at_utc               TIMESTAMPTZ      NOT NULL,
    loaded_at_utc                  TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT pk_curated_dengue_weekly PRIMARY KEY (loc, date)
);

-- One row per load into PostgreSQL: what happened, and how many rows changed.
CREATE TABLE IF NOT EXISTS load_audit (
    load_id          BIGSERIAL    PRIMARY KEY,
    pipeline_run_id  TEXT         NOT NULL,
    table_name       TEXT         NOT NULL,
    rows_in_file     INTEGER      NOT NULL,
    rows_inserted    INTEGER      NOT NULL,
    rows_updated     INTEGER      NOT NULL,
    rows_unchanged   INTEGER      NOT NULL,
    rows_in_table    INTEGER      NOT NULL,
    loaded_at_utc    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- One row per pipeline run: final status, row counts per layer, and the headline metric.
CREATE TABLE IF NOT EXISTS pipeline_run_summary (
    pipeline_run_id        TEXT             PRIMARY KEY,
    status                 TEXT             NOT NULL,
    started_at_utc         TIMESTAMPTZ,
    finished_at_utc        TIMESTAMPTZ,
    raw_rows               INTEGER,
    quarantined_rows       INTEGER,
    staging_rows           INTEGER,
    curated_rows           INTEGER,
    train_rows             INTEGER,
    test_rows              INTEGER,
    prediction_rows        INTEGER,
    primary_metric         TEXT,
    primary_metric_value   DOUBLE PRECISION,
    reproducibility_passed BOOLEAN,
    summary                JSONB            NOT NULL,
    published_at_utc       TIMESTAMPTZ      NOT NULL DEFAULT now()
);