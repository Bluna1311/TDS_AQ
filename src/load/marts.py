"""Marts: small summary tables for the dashboard, rebuilt from the warehouse on every run.

The fact table is persistent and loaded incrementally. Marts are different: they are
derived, small (thousands of rows, not millions) and take seconds to build, so each
run rebuilds them in full (CREATE OR REPLACE) and exports them to CSV for Tableau.

Four marts. Grain (one row =) and the dashboard questions each answers:
  mart_hourly_profile  year x season x weekday x UK hour x pollutant x site type
                       -> Which hours are worst? Does the season matter? Which pollutant?
  mart_daily_summary   UK date x pollutant
                       -> Is it getting better? (and the "Data up to" date)
  mart_station_summary station x pollutant, whole period
                       -> Which boroughs? What kind of place? Station count and coverage
  mart_health_bands    one row per index level 1-10 -> advice text
"""
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_FILE = PROJECT_ROOT / "data" / "warehouse.duckdb"
MARTS_DIR = PROJECT_ROOT / "data" / "marts"

# A station's month counts only if at least 75% of its hours have a real value.
# Patchy months would otherwise give a few lucky (or unlucky) hours too much weight.
COVERAGE_THRESHOLD = 0.75

# Moderate starts at index 4, High at 7 (the bands in dim_health_band).
MODERATE_FROM = 4
HIGH_FROM = 7

# ---------- building blocks, used by the marts below ----------
# Every reading, with its station, pollutant and UK time in words.
READINGS_VIEW_SQL = """
CREATE OR REPLACE TEMP VIEW readings_in_words AS
SELECT site.site_code, site.site_type, pollutant.species_code,
       hour_row.datetime_key, hour_row.uk_date, hour_row.uk_hour,
       hour_row.season, year(hour_row.uk_date) AS uk_year, month(hour_row.uk_date) AS uk_month,
       left(hour_row.weekday, 3) AS day_of_week,          -- Mon, Tue, ... (UK date)
       isodow(hour_row.uk_date)  AS day_of_week_number,   -- 1 = Monday ... 7 = Sunday, for sorting
       fact.value_ugm3, fact.aq_index
FROM fact_hourly_reading AS fact
JOIN dim_site      AS site      USING (site_key)
JOIN dim_pollutant AS pollutant USING (pollutant_key)
JOIN dim_datetime  AS hour_row  USING (datetime_key)
"""

# Share of hours with a real value, per station, pollutant and month.
COVERAGE_VIEW_SQL = """
CREATE OR REPLACE TEMP VIEW monthly_coverage AS
SELECT site_code, species_code, uk_year, uk_month,
       count(value_ugm3) / count(*) AS coverage
FROM readings_in_words
GROUP BY site_code, species_code, uk_year, uk_month
"""

# Indexed readings from well-covered months only, plus an 'Overall' row per station-hour:
# the highest index of any pollutant that hour, which is how the UK index reports a place.
INDEXED_VIEW_SQL = f"""
CREATE OR REPLACE TEMP VIEW indexed_readings AS
WITH covered AS (
    SELECT reading.*
    FROM readings_in_words AS reading
    JOIN monthly_coverage AS coverage
      USING (site_code, species_code, uk_year, uk_month)
    WHERE coverage.coverage >= {COVERAGE_THRESHOLD}
      AND reading.aq_index IS NOT NULL
)
SELECT site_code, site_type, species_code AS pollutant,
       datetime_key, uk_date, uk_hour, day_of_week, day_of_week_number, season, uk_year, aq_index
FROM covered
UNION ALL
SELECT site_code, site_type, 'Overall' AS pollutant,
       datetime_key, uk_date, uk_hour, day_of_week, day_of_week_number, season, uk_year, max(aq_index) AS aq_index
FROM covered
GROUP BY site_code, site_type, datetime_key, uk_date, uk_hour, day_of_week, day_of_week_number, season, uk_year
"""

# ---------- the marts ----------
# Sums and counts, not just averages, so Tableau can re-aggregate correctly
# when filters combine rows: mean index = SUM(index_sum) / SUM(hours_indexed).
HOURLY_PROFILE_SQL = f"""
CREATE OR REPLACE TABLE mart_hourly_profile AS
SELECT uk_year, season, day_of_week_number, day_of_week, uk_hour, pollutant, site_type,
       count(*)                                          AS hours_indexed,
       sum(aq_index)                                     AS index_sum,
       count(*) FILTER (WHERE aq_index >= {MODERATE_FROM}) AS hours_moderate_or_worse,
       count(*) FILTER (WHERE aq_index >= {HIGH_FROM})     AS hours_high_or_worse,
       round(avg(aq_index), 3)                           AS mean_index
FROM indexed_readings
GROUP BY uk_year, season, day_of_week_number, day_of_week, uk_hour, pollutant, site_type
"""

DAILY_SUMMARY_SQL = f"""
CREATE OR REPLACE TABLE mart_daily_summary AS
WITH station_days AS (
    SELECT site_code, pollutant, uk_date, max(aq_index) AS daily_max_index
    FROM indexed_readings
    GROUP BY site_code, pollutant, uk_date
),
london_days AS (
    SELECT uk_date, pollutant,
           count(*)                                                    AS stations_reporting,
           max(daily_max_index)                                        AS worst_index,
           round(avg(daily_max_index), 2)                              AS mean_station_max_index,
           count(*) FILTER (WHERE daily_max_index >= {MODERATE_FROM})  AS stations_moderate_or_worse
    FROM station_days
    GROUP BY uk_date, pollutant
)
-- The band is looked up AFTER summing up the day, from the day's worst index.
SELECT london_days.*, band.band AS worst_band
FROM london_days
JOIN dim_health_band AS band ON band.band_key = london_days.worst_index
"""

STATION_SUMMARY_SQL = f"""
CREATE OR REPLACE TABLE mart_station_summary AS
WITH data_quality AS (
    SELECT site_code, species_code AS pollutant,
           min(uk_date) FILTER (WHERE value_ugm3 IS NOT NULL) AS first_reading_date,
           max(uk_date) FILTER (WHERE value_ugm3 IS NOT NULL) AS last_reading_date,
           count(*)                                           AS hours_in_warehouse,
           count(value_ugm3)                                  AS hours_with_value
    FROM readings_in_words
    GROUP BY site_code, species_code
    UNION ALL
    -- Overall: an hour counts as covered if ANY pollutant has a real value that hour
    SELECT site_code, 'Overall' AS pollutant,
           min(uk_date) FILTER (WHERE value_ugm3 IS NOT NULL),
           max(uk_date) FILTER (WHERE value_ugm3 IS NOT NULL),
           count(DISTINCT datetime_key),
           count(DISTINCT datetime_key) FILTER (WHERE value_ugm3 IS NOT NULL)
    FROM readings_in_words
    GROUP BY site_code
),
index_stats AS (
    SELECT site_code, pollutant,
           count(*)                                            AS hours_indexed,
           sum(aq_index)                                       AS index_sum,
           count(*) FILTER (WHERE aq_index >= {MODERATE_FROM}) AS hours_moderate_or_worse,
           max(aq_index)                                       AS highest_index
    FROM indexed_readings
    GROUP BY site_code, pollutant
)
SELECT site.site_code, site.site_name, site.site_type, site.borough,
       site.latitude, site.longitude,
       site.date_closed IS NULL AS is_open,
       index_stats.pollutant,
       data_quality.first_reading_date, data_quality.last_reading_date,
       data_quality.hours_in_warehouse, data_quality.hours_with_value,
       round(data_quality.hours_with_value / data_quality.hours_in_warehouse, 3) AS coverage,
       index_stats.hours_indexed, index_stats.index_sum,
       round(index_stats.index_sum / index_stats.hours_indexed, 3)              AS mean_index,
       index_stats.hours_moderate_or_worse,
       round(index_stats.hours_moderate_or_worse / index_stats.hours_indexed, 4) AS share_moderate_or_worse,
       index_stats.highest_index
FROM index_stats
JOIN dim_site AS site ON site.site_code = index_stats.site_code AND site.is_current
LEFT JOIN data_quality
  ON data_quality.site_code = index_stats.site_code
 AND data_quality.pollutant = index_stats.pollutant
"""

HEALTH_BANDS_SQL = """
CREATE OR REPLACE TABLE mart_health_bands AS
SELECT aq_index, band, advice_general, advice_at_risk
FROM dim_health_band
"""

# Mart name -> (SQL that builds it, column order for the CSV)
MARTS = {
    "mart_hourly_profile":  (HOURLY_PROFILE_SQL,  "uk_year, season, day_of_week_number, uk_hour, pollutant, site_type"),
    "mart_daily_summary":   (DAILY_SUMMARY_SQL,   "uk_date, pollutant"),
    "mart_station_summary": (STATION_SUMMARY_SQL, "site_code, pollutant"),
    "mart_health_bands":    (HEALTH_BANDS_SQL,    "aq_index"),
}


def build_marts(connection):
    """Rebuild every mart table inside the warehouse."""
    for view_sql in (READINGS_VIEW_SQL, COVERAGE_VIEW_SQL, INDEXED_VIEW_SQL):
        connection.execute(view_sql)
    for build_sql, _ in MARTS.values():
        connection.execute(build_sql)


def export_marts(connection, marts_dir=MARTS_DIR):
    """Write each mart to <marts_dir>/<mart>.csv, via a temp file so a crash never leaves half a CSV."""
    marts_dir.mkdir(parents=True, exist_ok=True)
    row_counts = {}
    for mart_name, (_, sort_columns) in MARTS.items():
        csv_file = marts_dir / f"{mart_name}.csv"
        temp_file = csv_file.with_suffix(".tmp")
        connection.execute(
            f"COPY (SELECT * FROM {mart_name} ORDER BY {sort_columns}) "
            f"TO '{temp_file.as_posix()}' (HEADER, DELIMITER ',')"
        )
        temp_file.replace(csv_file)
        row_counts[mart_name] = connection.execute(f"SELECT count(*) FROM {mart_name}").fetchall()[0][0]
    return row_counts


def refresh_marts(warehouse_file=WAREHOUSE_FILE, marts_dir=MARTS_DIR):
    """Rebuild the marts in one transaction, then export them as CSV."""
    with duckdb.connect(str(warehouse_file)) as connection:
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute("BEGIN TRANSACTION")
        try:
            build_marts(connection)
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")   # the old marts stay as they were
            raise
        row_counts = export_marts(connection, marts_dir)

    for mart_name, row_count in row_counts.items():
        print(f"{mart_name}: {row_count:,} rows -> {mart_name}.csv")


if __name__ == "__main__":
    refresh_marts()