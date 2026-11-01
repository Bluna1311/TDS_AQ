from pathlib import Path

import pandas as pd

from src.transform.sites import write_parquet_safely

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STAGED_READINGS_DIR = PROJECT_ROOT / "data" / "staged" / "readings"
CLEANED_READINGS_DIR = PROJECT_ROOT / "data" / "cleaned" / "readings"

TARGET_SPECIES = ["NO2", "O3", "PM10", "PM25"]


def clean_readings(staged_dir=STAGED_READINGS_DIR, cleaned_dir=CLEANED_READINGS_DIR):
    """Keep only the four pollutants this project uses, one file per year."""
    for staged_file in sorted(staged_dir.glob("year=*/readings.parquet")):
        staged_readings = pd.read_parquet(staged_file)

        is_target = staged_readings["species_code"].isin(TARGET_SPECIES)
        cleaned_readings = staged_readings[is_target].reset_index(drop=True)

        year_folder = staged_file.parent.name          # e.g. "year=2024"
        write_parquet_safely(cleaned_readings, cleaned_dir / year_folder / "readings.parquet")
        print(f"{year_folder}: kept {len(cleaned_readings):,} of {len(staged_readings):,} rows")


if __name__ == "__main__":
    clean_readings()