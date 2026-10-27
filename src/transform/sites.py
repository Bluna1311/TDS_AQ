import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.transform.readings import as_list

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_SITES_DIR = PROJECT_ROOT / "data" / "raw" / "sites"
STAGED_SITES_FILE = PROJECT_ROOT / "data" / "staged" / "sites" / "stations.parquet"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H%M%SZ"


def parse_api_datetime(text):
    """'2006-09-10 00:00:00' -> datetime; '' -> None (no date)."""
    if not text:
        return None
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")


def parse_coordinate(text):
    """'51.5150461674013' -> 51.5150461674013; '' -> None."""
    if not text:
        return None
    return float(text)


def newest_raw_sites_file(raw_sites_dir=RAW_SITES_DIR):
    raw_files = sorted(raw_sites_dir.glob("sites_*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No station list in {raw_sites_dir}; run the extract first")
    return raw_files[-1]


def parse_sites_file(raw_sites_file):
    """Turn the raw station list into a table: one row per entry in the list."""
    response = json.loads(raw_sites_file.read_bytes())
    stamp = raw_sites_file.stem.removeprefix("sites_")
    fetched_at = datetime.strptime(stamp, TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)

    station_rows = []
    for site in as_list(response["Sites"]["Site"]):
        station_rows.append({
            "site_code": site["@SiteCode"],
            "site_name": site["@SiteName"],
            "site_type": site["@SiteType"],
            "borough": site["@LocalAuthorityName"],
            "latitude": parse_coordinate(site["@Latitude"]),
            "longitude": parse_coordinate(site["@Longitude"]),
            "date_opened": parse_api_datetime(site["@DateOpened"]),
            "date_closed": parse_api_datetime(site["@DateClosed"]),
        })

    stations = pd.DataFrame(station_rows)
    stations["fetched_at"] = fetched_at
    return stations


def merge_split_stations(stations):
    """One row per station. Some stations are split across several identical entries."""
    distinct_versions = stations.drop_duplicates()
    versions_per_station = distinct_versions.groupby("site_code").size()
    conflicting_codes = list(versions_per_station[versions_per_station > 1].index)
    if conflicting_codes:
        raise ValueError(f"Stations with conflicting details: {conflicting_codes}")
    return distinct_versions.sort_values("site_code").reset_index(drop=True)


def write_parquet_safely(table, target_file):
    """Write to a temporary file, then swap it in, so a crash never leaves half a file."""
    target_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file = target_file.with_suffix(".tmp")
    table.to_parquet(temp_file, index=False)
    temp_file.replace(target_file)


def stage_sites(raw_sites_dir=RAW_SITES_DIR, staged_sites_file=STAGED_SITES_FILE):
    """Stage the newest station list as stations.parquet: one row per station."""
    raw_sites_file = newest_raw_sites_file(raw_sites_dir)
    stations = parse_sites_file(raw_sites_file)
    stations = merge_split_stations(stations)
    write_parquet_safely(stations, staged_sites_file)
    print(f"Staged {len(stations)} stations from {raw_sites_file.name}")


if __name__ == "__main__":
    stage_sites()