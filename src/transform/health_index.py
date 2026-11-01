from pathlib import Path

import numpy as np
import pandas as pd

from src.transform.sites import write_parquet_safely

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLEANED_READINGS_DIR = PROJECT_ROOT / "data" / "cleaned" / "readings"
INDEXED_READINGS_DIR = PROJECT_ROOT / "data" / "cleaned" / "readings_with_index"

# Lower limit (ug/m3) of index 1 to 10 for each pollutant.
# Source: LondonAir, "How the air quality bands are calculated (1st January 2012 onwards)"
# https://www.londonair.org.uk/london/asp/airpollutionindex.asp
INDEX_LOWER_LIMITS = {
    "NO2":  [0, 68, 135, 201, 268, 335, 401, 468, 535, 601],
    "O3":   [0, 34, 67, 101, 121, 141, 161, 188, 214, 241],
    "PM10": [0, 17, 34, 51, 59, 67, 76, 84, 92, 101],
    "PM25": [0, 12, 24, 36, 42, 48, 54, 59, 65, 71],
}

# How each pollutant is averaged before banding, and how many real hours
# that window needs before an index is given (the data capture rule).
AVERAGING_RULES = {
    "NO2":  {"window": "1h",  "min_real_hours": 1},
    "O3":   {"window": "8h",  "min_real_hours": 6},
    "PM10": {"window": "24h", "min_real_hours": 18},
    "PM25": {"window": "24h", "min_real_hours": 18},
}


def round_half_up(values):
    """33.5 -> 34. (Python and pandas round 34.5 to 34, 'to even'; LondonAir means nearest.)"""
    return np.floor(values + 0.5)


def value_to_index(rounded_values, lower_limits):
    """Which index each rounded value falls in: the highest band whose lower limit it reaches."""
    index = np.searchsorted(lower_limits, rounded_values, side="right")
    index = np.clip(index, 1, 10).astype("float")
    index[np.isnan(rounded_values)] = np.nan   # no value means no index, never index 10
    return index


def add_health_index(readings):
    """Add index_value (the averaged value) and aq_index (1-10) to every reading."""
    readings = readings.sort_values(["site_code", "species_code", "measured_at_utc"])
    results = []
    for (site_code, species_code), series in readings.groupby(["site_code", "species_code"]):
        rules = AVERAGING_RULES[species_code]
        values_by_time = series.set_index("measured_at_utc")["value_ugm3"]

        averaged = values_by_time.rolling(rules["window"], min_periods=rules["min_real_hours"]).mean()

        series = series.copy()
        series["index_value"] = averaged.to_numpy()
        rounded = round_half_up(series["index_value"].to_numpy())
        series["aq_index"] = value_to_index(rounded, INDEX_LOWER_LIMITS[species_code])
        results.append(series)

    indexed = pd.concat(results, ignore_index=True)
    indexed["aq_index"] = indexed["aq_index"].astype("Int64")   # whole numbers, NULL allowed
    return indexed


def index_readings(cleaned_dir=CLEANED_READINGS_DIR, indexed_dir=INDEXED_READINGS_DIR):
    """Load every cleaned year together (averages cross New Year), add the index, save per year."""
    cleaned_files = sorted(cleaned_dir.glob("year=*/readings.parquet"))
    readings = pd.concat([pd.read_parquet(cleaned_file) for cleaned_file in cleaned_files],
                         ignore_index=True)

    indexed = add_health_index(readings)

    for year, readings_for_year in indexed.groupby(indexed["measured_at_utc"].dt.year):
        output_file = indexed_dir / f"year={year}" / "readings.parquet"
        write_parquet_safely(readings_for_year.reset_index(drop=True), output_file)
        with_index = readings_for_year["aq_index"].notna().sum()
        print(f"year={year}: {len(readings_for_year):,} rows, {with_index:,} with an index")


if __name__ == "__main__":
    index_readings()