from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.extract.api import TIMESTAMP_FORMAT, fetch

SITES_PATH = "/Information/MonitoringSiteSpecies/GroupName=London/Json"
RAW_DIR = Path("data/raw/sites")
SITES_MAX_AGE = timedelta(days=1)


def latest_fetch(raw_dir):
    """Return (path, fetched_at) for the newest saved file, or (None, None)."""
    files = sorted(raw_dir.glob("sites_*.json"))
    if not files:
        return None, None
    newest = files[-1]
    stamp = newest.stem.removeprefix("sites_")
    fetched_at = datetime.strptime(stamp, TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
    return newest, fetched_at


def extract_sites(raw_dir=RAW_DIR, max_age=SITES_MAX_AGE):

    raw_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)

    newest, fetched_at = latest_fetch(raw_dir)
    if newest and now - fetched_at < max_age:
        print(f"Skipping: last fetched {fetched_at:%Y-%m-%d %H:%M} UTC, still fresh")
        return newest

    content = fetch(SITES_PATH)
    path = raw_dir / f"sites_{now.strftime(TIMESTAMP_FORMAT)}.json"
    path.write_bytes(content)
    return path


if __name__ == "__main__":
    saved = extract_sites()
    print(f"Using {saved}")