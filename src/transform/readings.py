import json
from pathlib import Path

import pandas as pd

from src.extract.readings import RAW_DIR, fetched_at_from_path

STAGED_DIR = Path("data/staged/readings")
READING_KEY = ["site_code", "species_code", "measured_at_utc"]


def as_list(value):
    """The API returns a single item as an object and several as a list."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def parse_raw_file(raw_file):
    response = json.loads(raw_file.read_bytes())["AirQualityData"]
    site_code = response["@SiteCode"]

    rows = []
    for reading in as_list(response.get("Data")):
        row = {
            "site_code": site_code,
            "species_code": reading["@SpeciesCode"],
            "measured_at_utc": reading["@MeasurementDateGMT"],
            "value_ugm3": reading["@Value"],
        }
        rows.append(row)

    readings = pd.DataFrame(rows, columns=READING_KEY + ["value_ugm3"])
    readings["measured_at_utc"] = pd.to_datetime(readings["measured_at_utc"], utc=True)
    # Blank or non-numeric means no valid reading: store NULL, never 0.
    readings["value_ugm3"] = pd.to_numeric(readings["value_ugm3"].replace("", None), errors="coerce")
    readings["fetched_at"] = pd.Timestamp(fetched_at_from_path(raw_file))
    return readings


def partition_path(staged_dir, year):
    return staged_dir / f"year={year}" / "readings.parquet"


def merge_into_staging(new_readings, staged_dir=STAGED_DIR):

    years = new_readings["measured_at_utc"].dt.year
    for year, new_for_year in new_readings.groupby(years):
        year_file = partition_path(staged_dir, year)

        if year_file.exists():
            already_staged = pd.read_parquet(year_file)
            combined = pd.concat([already_staged, new_for_year], ignore_index=True)
        else:
            combined = new_for_year

        latest_only = (
            combined.sort_values("fetched_at")
                    .drop_duplicates(subset=READING_KEY, keep="last")
                    .sort_values(READING_KEY)
                    .reset_index(drop=True)
        )

        year_file.parent.mkdir(parents=True, exist_ok=True)
        temp_file = year_file.with_suffix(".tmp")
        latest_only.to_parquet(temp_file, index=False)
        temp_file.replace(year_file)


def stage_new_raw_files(raw_dir=RAW_DIR, staged_dir=STAGED_DIR):
    manifest = staged_dir / "staged_files.txt"
    if manifest.exists():
        already_staged_names = set(manifest.read_text().splitlines())
    else:
        already_staged_names = set()

    files_to_stage = []
    for raw_file in raw_dir.rglob("*.json"):
        if raw_file.name not in already_staged_names:
            files_to_stage.append(raw_file)
    files_to_stage = sorted(files_to_stage)

    if not files_to_stage:
        print("Nothing new to stage")
        return

    tables = []
    for raw_file in files_to_stage:
        tables.append(parse_raw_file(raw_file))
    new_readings = pd.concat(tables, ignore_index=True)

    merge_into_staging(new_readings, staged_dir)

    staged_dir.mkdir(parents=True, exist_ok=True)
    with manifest.open("a") as manifest_file:
        for raw_file in files_to_stage:
            manifest_file.write(raw_file.name + "\n")
    print(f"Staged {len(files_to_stage)} file(s), {len(new_readings):,} rows")


if __name__ == "__main__":
    stage_new_raw_files()