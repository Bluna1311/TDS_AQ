from pathlib import Path

import duckdb

from src.load.schema import create_schema

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_FILE = PROJECT_ROOT / "data" / "warehouse.duckdb"
STATIONS_FILE = PROJECT_ROOT / "data" / "staged" / "sites" / "stations.parquet"
HEALTH_ADVICE_FILE = PROJECT_ROOT / "data" / "staged" / "health_advice" / "health_advice.parquet"
INDEXED_READINGS = PROJECT_ROOT / "data" / "cleaned" / "readings_with_index"

# Each run re-loads the most recent days, because those are the only hours that
# can have changed: the extract re-fetches the last 14 days (late corrections),
# and a 24-hour average needs the day before. Two days' margin on top.
RELOAD_WINDOW_DAYS = 16

STATION_DETAIL_COLUMNS = ["site_name", "site_type", "borough", "latitude", "longitude",
                          "date_opened", "date_closed"]


def readings_source():
    return f"read_parquet('{INDEXED_READINGS.as_posix()}/*/*.parquet')"


# ---------- small dimensions: insert if missing, update if changed ----------
UPSERT_POLLUTANTS_SQL = """
INSERT INTO dim_pollutant VALUES
    (1, 'NO2',  'Nitrogen dioxide',  '1 hour'),
    (2, 'O3',   'Ozone',             '8-hour mean'),
    (3, 'PM10', 'PM10 particulate',  '24-hour mean'),
    (4, 'PM25', 'PM2.5 particulate', '24-hour mean')
ON CONFLICT (pollutant_key) DO UPDATE SET
    species_code = EXCLUDED.species_code,
    species_name = EXCLUDED.species_name,
    index_averaging = EXCLUDED.index_averaging;
"""

UPSERT_HEALTH_BANDS_SQL = f"""
INSERT INTO dim_health_band
SELECT index_level, index_level, advice.band, advice.advice_general, advice.advice_at_risk
FROM range(1, 11) AS levels(index_level)
JOIN read_parquet('{HEALTH_ADVICE_FILE.as_posix()}') AS advice
  ON index_level BETWEEN advice.lower_index AND advice.upper_index
ON CONFLICT (band_key) DO UPDATE SET
    band = EXCLUDED.band,
    advice_general = EXCLUDED.advice_general,
    advice_at_risk = EXCLUDED.advice_at_risk;
"""


def add_new_hours(connection, load_from):
    """Add any hour not yet in dim_datetime. An hour's key never changes."""
    connection.execute(f"""
        INSERT INTO dim_datetime
        WITH new_hours AS (
            SELECT unnest(generate_series(
                       greatest((SELECT min(measured_at_utc) FROM {readings_source()}), ?),
                       (SELECT max(measured_at_utc) FROM {readings_source()}),
                       INTERVAL 1 HOUR)) AS measured_at_utc
        ),
        with_uk_time AS (
            SELECT measured_at_utc, timezone('Europe/London', measured_at_utc) AS uk_time
            FROM new_hours
        )
        SELECT CAST(epoch(measured_at_utc) / 3600 AS INTEGER) AS datetime_key,
               measured_at_utc,
               CAST(uk_time AS DATE), hour(uk_time), dayname(uk_time),
               isodow(uk_time) >= 6, month(uk_time),
               CASE WHEN month(uk_time) IN (12, 1, 2) THEN 'Winter'
                    WHEN month(uk_time) IN (3, 4, 5)  THEN 'Spring'
                    WHEN month(uk_time) IN (6, 7, 8)  THEN 'Summer'
                    ELSE 'Autumn' END,
               hour(uk_time) <> hour(measured_at_utc)
        FROM with_uk_time
        ON CONFLICT (datetime_key) DO NOTHING
    """, [load_from])


# ---------- dim_site: SCD type 2 ----------
def update_stations(connection):
    """Close the current version of any station whose details changed, then add
    a new version for it, and a first version for any new station."""
    details_changed = " OR ".join(
        f"current_version.{column} IS DISTINCT FROM latest.{column}" for column in STATION_DETAIL_COLUMNS)

    closed = connection.execute(f"""
        UPDATE dim_site AS current_version
        SET valid_to = latest.fetched_at, is_current = false
        FROM read_parquet('{STATIONS_FILE.as_posix()}') AS latest
        WHERE current_version.site_code = latest.site_code
          AND current_version.is_current
          AND ({details_changed})
    """).fetchall()[0][0]

    # A station's FIRST version is valid from 1900, so all its past readings link to it.
    # Later versions are valid from when the change was seen in the station list.
    added = connection.execute(f"""
        INSERT INTO dim_site
        SELECT (SELECT coalesce(max(site_key), 0) FROM dim_site)
                   + row_number() OVER (ORDER BY latest.site_code) AS site_key,
               latest.site_code, latest.site_name, latest.site_type, latest.borough,
               latest.latitude, latest.longitude, latest.date_opened, latest.date_closed,
               CASE WHEN latest.site_code IN (SELECT site_code FROM dim_site)
                    THEN latest.fetched_at
                    ELSE TIMESTAMPTZ '1900-01-01 00:00:00+00' END AS valid_from,
               NULL AS valid_to,
               true AS is_current
        FROM read_parquet('{STATIONS_FILE.as_posix()}') AS latest
        WHERE latest.site_code NOT IN (SELECT site_code FROM dim_site WHERE is_current)
    """).fetchall()[0][0]
    return closed, added


# ---------- fact: reload the recent window ----------
def readings_to_load_sql(load_from):
    """Readings measured on or after load_from, with their keys looked up.
    Each reading links to the station version that was true when it was measured."""
    return f"""
        SELECT site.site_key,
               pollutant.pollutant_key,
               CAST(epoch(reading.measured_at_utc) / 3600 AS INTEGER) AS datetime_key,
               reading.aq_index AS band_key,
               reading.value_ugm3, reading.index_value, reading.aq_index
        FROM {readings_source()} AS reading
        JOIN dim_site AS site
          ON site.site_code = reading.site_code
         AND reading.measured_at_utc >= site.valid_from
         AND (site.valid_to IS NULL OR reading.measured_at_utc < site.valid_to)
        JOIN dim_pollutant AS pollutant ON pollutant.species_code = reading.species_code
        WHERE reading.measured_at_utc >= TIMESTAMPTZ '{load_from}'
    """


def reload_readings(connection, load_from):
    """Replace the fact rows for the reload window with the latest readings.

    Only hours on or after load_from are touched; older history is never changed.
    The window is deleted and re-inserted (not updated in place) because a reading
    can move to a NEW station version (SCD type 2): updating in place would leave
    the old row behind and count the same hour twice.
    """
    source_rows = connection.execute(
        f"SELECT count(*) FROM {readings_source()} WHERE measured_at_utc >= TIMESTAMPTZ '{load_from}'"
    ).fetchall()[0][0]
    matched_rows = connection.execute(
        f"SELECT count(*) FROM ({readings_to_load_sql(load_from)})").fetchall()[0][0]
    if matched_rows != source_rows:
        raise ValueError(f"{source_rows - matched_rows:,} readings did not match a station version or pollutant")

    connection.execute(f"""
        DELETE FROM fact_hourly_reading
        WHERE datetime_key >= floor(epoch(TIMESTAMPTZ '{load_from}') / 3600)
    """)
    connection.execute(f"INSERT INTO fact_hourly_reading {readings_to_load_sql(load_from)}")

    duplicate_hours = connection.execute("""
        SELECT count(*) - count(DISTINCT (site.site_code, fact.pollutant_key, fact.datetime_key))
        FROM fact_hourly_reading AS fact JOIN dim_site AS site USING (site_key)
    """).fetchall()[0][0]
    if duplicate_hours:
        raise ValueError(f"{duplicate_hours:,} station + pollutant + hour combinations appear twice")
    return source_rows


def load_warehouse(warehouse_file=WAREHOUSE_FILE, reload_window_days=RELOAD_WINDOW_DAYS):
    """Add the latest data to the warehouse in one transaction: all of it, or none of it."""
    warehouse_file.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(warehouse_file)) as connection:
        connection.execute("SET TimeZone = 'UTC'")
        create_schema(connection)  # only creates tables that are missing

        fact_rows_before = connection.execute("SELECT count(*) FROM fact_hourly_reading").fetchall()[0][0]
        if fact_rows_before == 0:
            load_from = "1900-01-01 00:00:00+00"  # first run: load everything
        else:
            # Reload the recent window, or further back if the warehouse is further
            # behind than that (e.g. the daily run was not run for a few weeks).
            load_from = connection.execute(f"""
                SELECT least(
                    (SELECT max(measured_at_utc) FROM {readings_source()}) - INTERVAL {reload_window_days} DAYS,
                    (SELECT max(hour_row.measured_at_utc)
                     FROM fact_hourly_reading AS fact
                     JOIN dim_datetime AS hour_row USING (datetime_key)))
            """).fetchall()[0][0].isoformat()

        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(UPSERT_POLLUTANTS_SQL)
            connection.execute(UPSERT_HEALTH_BANDS_SQL)
            add_new_hours(connection, load_from)
            versions_closed, versions_added = update_stations(connection)
            readings_loaded = reload_readings(connection, load_from)
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")  # the warehouse is left exactly as it was
            raise

        fact_rows_after = connection.execute("SELECT count(*) FROM fact_hourly_reading").fetchall()[0][0]
        mode = "first load" if fact_rows_before == 0 else f"reloaded from {load_from[:16]} UTC"
        print(f"{mode}: {readings_loaded:,} readings loaded")
        print(f"stations: {versions_added} versions added, {versions_closed} closed")
        print(f"fact_hourly_reading: {fact_rows_before:,} -> {fact_rows_after:,} rows")


if __name__ == "__main__":
    load_warehouse()