from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.extract.api import TIMESTAMP_FORMAT, fetch

RAW_DIR = Path("data/raw/readings")
RECENT_MAX_AGE = timedelta(days=1)


def raw_filename(site_code, start, end, fetched_at):
    """e.g. TH4_2023-01-01_2024-01-01_fetched_2026-10-24T020000Z.json"""
    return f"{site_code}_{start}_{end}_fetched_{fetched_at.strftime(TIMESTAMP_FORMAT)}.json"


def fetched_at_from_path(path):
    stamp = path.stem.split("_fetched_")[1]
    return datetime.strptime(stamp, TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)


def latest_raw_file(site_dir, site_code, start, end):
    files = sorted(site_dir.glob(f"{site_code}_{start}_{end}_fetched_*.json"))
    return files[-1] if files else None


def extract_readings(site_code, start, end, raw_dir=RAW_DIR, max_age=RECENT_MAX_AGE):
    """Fetch hourly readings for one site and date range, and save them raw.

    start is inclusive and end is exclusive, as the API treats them.

    Skips the API call when:
      - the period is finished (end is in the past) and we already have it, or
      - the period is recent and our latest copy is younger than max_age.
    Returns the path of the file to use.
    """
    site_dir = raw_dir / f"site={site_code}"
    site_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)

    existing = latest_raw_file(site_dir, site_code, start, end)
    if existing:
        period_finished = end <= now.date() - timedelta(days=30)
        still_fresh = now - fetched_at_from_path(existing) < max_age
        if period_finished or still_fresh:
            return existing

    content = fetch(f"/Data/Site/SiteCode={site_code}/StartDate={start}/EndDate={end}/Json")
    path = site_dir / raw_filename(site_code, start, end, now)
    path.write_bytes(content)
    return path


if __name__ == "__main__":
    saved = extract_readings("TH4", date(2025, 7, 7), date(2025, 7, 14))
    print(f"Using {saved}")