import json
from pathlib import Path

import pandas as pd

from src.extract.readings import RAW_DIR, fetched_at_from_path

STAGED_DIR = Path("data/staged/readings")
KEY = ["site_code", "species_code", "measured_at_utc"]


def as_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def parse_raw_file(path):
    data = json.loads(path.read_bytes())["AirQualityData"]
    site_code = data["@SiteCode"]

    rows = []
    for point in as_list(data.get("Data")):
        row = {
            "site_code": site_code,
            "species_code": point["@SpeciesCode"],
            "measured_at_utc": point["@MeasurementDateGMT"],
            "value_ugm3": point["@Value"],
        }
        rows.append(row)

    df = pd.DataFrame(rows, columns=KEY + ["value_ugm3"])
    df["measured_at_utc"] = pd.to_datetime(df["measured_at_utc"], utc=True)
    # Blank or non-numeric means no valid reading: store NULL, never 0.
    df["value_ugm3"] = pd.to_numeric(df["value_ugm3"].replace("", None), errors="coerce")
    df["fetched_at"] = pd.Timestamp(fetched_at_from_path(path))
    return df


def partition_path(staged_dir, year):
    return staged_dir / f"year={year}" / "readings.parquet"

