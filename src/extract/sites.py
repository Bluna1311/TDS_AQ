import json
from datetime import datetime, timezone
from pathlib import Path
import requests

BASE_URL = "https://api.erg.ic.ac.uk/AirQuality"
SITES_PATH = "/Information/MonitoringSiteSpecies/GroupName=London/Json"
RAW_DIR = Path("data/raw/sites")
TIMEOUT_SECONDS = 120


def fetch(path):
    response = requests.get(f"{BASE_URL}{path}", timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    json.loads(response.content)
    return response.content


def extract_sites(raw_dir=RAW_DIR):
    content = fetch(SITES_PATH)
    raw_dir.mkdir(parents=True, exist_ok=True)
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    path = raw_dir / f"sites_{fetched_at}.json"
    path.write_bytes(content)
    return path


if __name__ == "__main__":
    saved = extract_sites()
    print(f"Saved {saved} ({saved.stat().st_size:,} bytes)")