from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_FILE = PROJECT_ROOT / "data" / "warehouse.duckdb"

# Star schema: one fact table of hourly readings, four dimensions around it.
# The warehouse is persistent: tables are created once and data is added to
# them run by run. A change to a definition here needs a migration (or a
# deliberate rebuild); IF NOT EXISTS will not alter an existing table.
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_site (
    site_key        INTEGER PRIMARY KEY,   -- one per station VERSION (SCD type 2)
    site_code       VARCHAR NOT NULL,      -- natural key, e.g. TH4
    site_name       VARCHAR,
    site_type       VARCHAR,               -- Roadside, Urban Background, ...
    borough         VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    date_opened     TIMESTAMP,
    date_closed     TIMESTAMP,             -- NULL = still open
    valid_from      TIMESTAMPTZ NOT NULL,  -- this version is true from...
    valid_to        TIMESTAMPTZ,           -- ...until (NULL = current version)
    is_current      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_pollutant (
    pollutant_key   INTEGER PRIMARY KEY,
    species_code    VARCHAR NOT NULL,      -- NO2, O3, PM10, PM25
    species_name    VARCHAR,
    index_averaging VARCHAR                -- 1 hour, 8-hour mean, 24-hour mean
);

CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_key    INTEGER PRIMARY KEY,   -- hours since 1970-01-01 UTC: stable, never renumbered
    measured_at_utc TIMESTAMPTZ NOT NULL,
    uk_date         DATE,
    uk_hour         INTEGER,               -- 0-23, UK local time
    weekday         VARCHAR,
    is_weekend      BOOLEAN,
    month           INTEGER,
    season          VARCHAR,               -- Winter, Spring, Summer, Autumn
    is_bst          BOOLEAN                -- British Summer Time in force
);

CREATE TABLE IF NOT EXISTS dim_health_band (
    band_key        INTEGER PRIMARY KEY,   -- equals aq_index, 1-10
    aq_index        INTEGER NOT NULL,
    band            VARCHAR,               -- Low, Moderate, High, Very High
    advice_general  VARCHAR,
    advice_at_risk  VARCHAR
);

-- Grain: one row per station x pollutant x UTC hour (the primary key).
-- site_key -> dim_site, pollutant_key -> dim_pollutant,
-- datetime_key -> dim_datetime, band_key -> dim_health_band (NULL when no index)
CREATE TABLE IF NOT EXISTS fact_hourly_reading (
    site_key        INTEGER NOT NULL,
    pollutant_key   INTEGER NOT NULL,
    datetime_key    INTEGER NOT NULL,
    band_key        INTEGER,
    value_ugm3      DOUBLE,                -- NULL = missing reading
    index_value     DOUBLE,                -- the averaged value the index is based on
    aq_index        INTEGER,               -- 1-10, NULL = not enough data
    PRIMARY KEY (site_key, pollutant_key, datetime_key)
);
"""


def create_schema(connection):
    """Create the five tables if they don't exist yet. Never deletes data."""
    connection.execute(SCHEMA_SQL)


if __name__ == "__main__":
    WAREHOUSE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE_FILE)) as warehouse_connection:
        create_schema(warehouse_connection)
        table_names = [row[0] for row in warehouse_connection.execute("SHOW TABLES").fetchall()]
    print(f"Tables in {WAREHOUSE_FILE.name}: {', '.join(table_names)}")
