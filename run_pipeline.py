"""Backfill historical readings, politely and resumably.

Steps:
  1. Extract the London station list (skipped if fetched within the last day).
  2. Work out the jobs from the station list: one API call per station
     per chunk of months, only where a target pollutant was measured.
  3. Run the jobs, pausing between calls, up to MAX_CALLS_PER_RUN.
     Jobs already held on disk are skipped without an API call, so
     re-running carries on where the last run stopped.
  4. Stage every new raw file into Parquet.

Run from the repo root:
    python run_backfill.py
"""
import json
import time
from datetime import date, datetime

import requests

from src.extract.readings import RAW_DIR, extract_readings, latest_raw_file
from src.extract.sites import extract_sites
from src.transform.readings import stage_new_raw_files

BACKFILL_START = date(2021, 1, 1)
BACKFILL_END = date.today().replace(day=1)   # up to the start of this month
MONTHS_PER_CHUNK = 1                         # one month per call: tested at ~3s
PAUSE_SECONDS = 3                            # wait between API calls
MAX_CALLS_PER_RUN = 1500                     # stop after this many calls; re-run to continue
MAX_CONSECUTIVE_FAILURES = 3                 # stop if the API keeps failing
TARGET_SPECIES = {"NO2", "O3", "PM25", "PM10"}


# ---------- deciding what to fetch ----------
def as_list(value):
    """The API returns a single item as an object and several as a list."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def parse_api_date(text):
    """'2006-09-10 00:00:00' -> date(2006, 9, 10); '' -> None (no date)."""
    if not text:
        return None
    return datetime.strptime(text[:10], "%Y-%m-%d").date()


def load_measurement_periods(sites_file):
    """One row per station and pollutant: when that pollutant was measured there."""
    response = json.loads(sites_file.read_bytes())

    measurement_periods = []
    for site in as_list(response["Sites"]["Site"]):
        for species in as_list(site.get("Species")):
            measurement_periods.append({
                "site_code": site["@SiteCode"],
                "species_code": species["@SpeciesCode"],
                "measured_from": parse_api_date(species["@DateMeasurementStarted"]),
                "measured_until": parse_api_date(species["@DateMeasurementFinished"]),
            })
    return measurement_periods


def add_months(first_of_month, months):
    """date(2025, 11, 1) + 3 months -> date(2026, 2, 1)."""
    month_index = first_of_month.month - 1 + months
    return date(first_of_month.year + month_index // 12, month_index % 12 + 1, 1)


def month_chunks(start, end, months_per_chunk=1):
    """Split start..end into chunks of whole months. start must be the 1st of a month."""
    chunks = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(add_months(chunk_start, months_per_chunk), end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end
    return chunks


def overlaps(measurement_period, chunk_start, chunk_end):
    """True if this pollutant was being measured at any point in the chunk."""
    started = measurement_period["measured_from"]
    finished = measurement_period["measured_until"]
    started_before_chunk_ends = started is None or started < chunk_end
    still_going_when_chunk_starts = finished is None or finished > chunk_start
    return started_before_chunk_ends and still_going_when_chunk_starts


def plan_backfill(measurement_periods, start, end, months_per_chunk=1):
    """One job per station per chunk, only where a target pollutant was measured."""
    target_periods = []
    for measurement_period in measurement_periods:
        if measurement_period["species_code"] in TARGET_SPECIES:
            target_periods.append(measurement_period)

    site_codes = sorted({measurement_period["site_code"] for measurement_period in target_periods})
    chunks = month_chunks(start, end, months_per_chunk)

    jobs = []
    for site_code in site_codes:
        site_periods = []
        for measurement_period in target_periods:
            if measurement_period["site_code"] == site_code:
                site_periods.append(measurement_period)

        for chunk_start, chunk_end in chunks:
            for measurement_period in site_periods:
                if overlaps(measurement_period, chunk_start, chunk_end):
                    jobs.append({"site_code": site_code, "start": chunk_start, "end": chunk_end})
                    break
    return jobs


# ---------- running the backfill ----------
def main():
    run_started = time.monotonic()

    print("1. Station list")
    sites_file = extract_sites()

    print("\n2. Planning")
    measurement_periods = load_measurement_periods(sites_file)
    jobs = plan_backfill(measurement_periods, BACKFILL_START, BACKFILL_END, MONTHS_PER_CHUNK)
    station_count = len({job["site_code"] for job in jobs})
    print(f"   {len(jobs):,} jobs across {station_count} stations, {BACKFILL_START} to {BACKFILL_END}")

    print("\n3. Fetching")
    calls_made = 0
    jobs_skipped = 0
    consecutive_failures = 0
    failed_jobs = []

    for job_number, job in enumerate(jobs, start=1):
        if calls_made >= MAX_CALLS_PER_RUN:
            print(f"   Reached {MAX_CALLS_PER_RUN} calls; re-run to continue")
            break

        label = f"[{job_number}/{len(jobs)}] {job['site_code']} {job['start']} to {job['end']}"
        site_dir = RAW_DIR / f"site={job['site_code']}"
        file_before = latest_raw_file(site_dir, job["site_code"], job["start"], job["end"])
        try:
            raw_file = extract_readings(job["site_code"], job["start"], job["end"])
        except (requests.RequestException, ValueError) as error:
            calls_made += 1
            consecutive_failures += 1
            failed_jobs.append(label)
            print(f"   {label} FAILED: {error}")
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print(f"   {MAX_CONSECUTIVE_FAILURES} failures in a row; stopping")
                break
            time.sleep(PAUSE_SECONDS)
            continue

        consecutive_failures = 0
        # A different file back means extract_readings called the API and saved a new one.
        fetched_this_run = raw_file != file_before
        if fetched_this_run:
            calls_made += 1
            print(f"   {label} fetched")
            time.sleep(PAUSE_SECONDS)
        else:
            jobs_skipped += 1

    print(f"\n   API calls: {calls_made}, already held: {jobs_skipped}, failed: {len(failed_jobs)}")
    for label in failed_jobs:
        print(f"   failed: {label}")

    print("\n4. Staging")
    stage_new_raw_files()

    remaining = len(jobs) - jobs_skipped - (calls_made - len(failed_jobs))
    print(f"\nJobs still to fetch: {remaining:,} of {len(jobs):,}")
    print(f"Run took {(time.monotonic() - run_started) / 60:.1f} minutes")


if __name__ == "__main__":
    main()