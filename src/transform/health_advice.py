from pathlib import Path
import xml.etree.ElementTree as ElementTree

import pandas as pd

from src.transform.sites import write_parquet_safely

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_HEALTH_ADVICE_DIR = PROJECT_ROOT / "data" / "raw" / "health_advice"
STAGED_HEALTH_ADVICE_FILE = PROJECT_ROOT / "data" / "staged" / "health_advice" / "health_advice.parquet"

POPULATION_COLUMNS = {
    "General population": "advice_general",
    "At-risk individuals": "advice_at_risk",
}


def newest_raw_health_advice_file(raw_dir=RAW_HEALTH_ADVICE_DIR):
    raw_files = sorted(raw_dir.glob("health_advice_*.xml"))
    if not raw_files:
        raise FileNotFoundError(f"No health advice in {raw_dir}; run the extract first")
    return raw_files[-1]


def parse_health_advice_file(raw_file):
    """One row per band: its index range and the advice for each population."""
    root = ElementTree.fromstring(raw_file.read_bytes())

    band_rows = []
    for banding in root.findall("AirQualityBanding"):
        band_row = {
            "band": banding.get("AirQualityBand"),
            "lower_index": int(banding.get("LowerAirQualityIndex")),
            "upper_index": int(banding.get("UpperAirQualityIndex")),
        }
        for advice in banding.findall("HealthAdvice"):
            column = POPULATION_COLUMNS[advice.get("Population")]
            band_row[column] = advice.get("Advice")
        band_rows.append(band_row)

    bands = pd.DataFrame(band_rows)
    missing_columns = set(POPULATION_COLUMNS.values()) - set(bands.columns)
    if len(bands) != 4 or missing_columns:
        raise ValueError(f"Unexpected health advice: {len(bands)} bands, missing {missing_columns}")
    return bands.sort_values("lower_index").reset_index(drop=True)


def stage_health_advice(raw_dir=RAW_HEALTH_ADVICE_DIR, staged_file=STAGED_HEALTH_ADVICE_FILE):
    raw_file = newest_raw_health_advice_file(raw_dir)
    bands = parse_health_advice_file(raw_file)
    write_parquet_safely(bands, staged_file)
    print(f"Staged {len(bands)} health advice bands from {raw_file.name}")


if __name__ == "__main__":
    stage_health_advice()