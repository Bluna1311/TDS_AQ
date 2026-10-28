from datetime import datetime, timedelta, timezone
from pathlib import Path
import xml.etree.ElementTree as ElementTree

from src.extract.api import TIMESTAMP_FORMAT, fetch

# The JSON version of this endpoint returns HTTP 400; the XML version works.
HEALTH_ADVICE_PATH = "/Information/IndexHealthAdvice"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_HEALTH_ADVICE_DIR = PROJECT_ROOT / "data" / "raw" / "health_advice"
HEALTH_ADVICE_MAX_AGE = timedelta(days=30)   # official wording rarely changes


def latest_fetch(raw_dir):
    """Return (path, fetched_at) for the newest saved file, or (None, None)."""
    raw_files = sorted(raw_dir.glob("health_advice_*.xml"))
    if not raw_files:
        return None, None
    newest = raw_files[-1]
    stamp = newest.stem.removeprefix("health_advice_")
    fetched_at = datetime.strptime(stamp, TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
    return newest, fetched_at


def extract_health_advice(raw_dir=RAW_HEALTH_ADVICE_DIR, max_age=HEALTH_ADVICE_MAX_AGE):
    """Fetch the official health advice per index band, unless a recent copy exists."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)

    newest, fetched_at = latest_fetch(raw_dir)
    if newest and now - fetched_at < max_age:
        print(f"Skipping: last fetched {fetched_at:%Y-%m-%d %H:%M} UTC, still fresh")
        return newest

    content = fetch(HEALTH_ADVICE_PATH, expect_json=False)
    ElementTree.fromstring(content)   # check it is real XML before saving
    path = raw_dir / f"health_advice_{now.strftime(TIMESTAMP_FORMAT)}.xml"
    path.write_bytes(content)
    return path


if __name__ == "__main__":
    saved = extract_health_advice()
    print(f"Using {saved}")