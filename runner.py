"""Daily run: API to warehouse to dashboard files, in one command.

Steps:
  1. Extract  - station list (if over a day old), health advice (if over 30 days old),
                and readings for the current month, plus last month while the
                14-day correction window still reaches into it.
  2. Stage    - new raw files into Parquet (latest fetch wins), stations, health advice.
  3. Clean    - keep our four pollutants.
  4. Index    - health index for every hour.
  5. Load     - add the latest data to the warehouse (only the recent window is reloaded).
  6. Marts    - rebuild the summary tables and export them as CSV for Tableau.

Run from the repo root:
    python runner.py
"""
import time
from datetime import date, timedelta

import requests

from src.extract.health_advice import extract_health_advice
from src.extract.planner import add_months, load_measurement_periods, plan_jobs
from src.extract.readings import RAW_DIR, extract_readings, latest_raw_file
from src.extract.sites import extract_sites
from src.load.marts import refresh_marts
from src.load.warehouse import load_warehouse
from src.transform.clean import clean_readings
from src.transform.health_advice import stage_health_advice
from src.transform.health_index import index_readings
from src.transform.readings import stage_new_raw_files
from src.transform.sites import stage_sites

CORRECTION_WINDOW_DAYS = 14     # ERG can revise readings for about this long
PAUSE_SECONDS = 1               # wait after each real API call
MAX_CONSECUTIVE_FAILURES = 3    # stop if the API keeps failing


def daily_window(today):
    """First of the month that contains (today - 14 days), up to the first of next month."""
    window_start = (today - timedelta(days=CORRECTION_WINDOW_DAYS)).replace(day=1)
    window_end = add_months(today.replace(day=1), 1)
    return window_start, window_end


def fetch_recent_readings(jobs):
    """Run each job. extract_readings skips any whose copy is under a day old;
    only real API calls are followed by a pause."""
    calls_made = 0
    consecutive_failures = 0
    failed_jobs = []
    for job_number, job in enumerate(jobs, start=1):
        site_code, start, end = job["site_code"], job["start"], job["end"]
        label = f"[{job_number}/{len(jobs)}] {site_code} {start} to {end}"
        file_before = latest_raw_file(RAW_DIR / f"site={site_code}", site_code, start, end)
        job_started = time.monotonic()
        try:
            file_used = extract_readings(site_code, start, end)
        except (requests.RequestException, ValueError) as error:
            consecutive_failures += 1
            failed_jobs.append(label)
            print(f"   {label} FAILED: {error}")
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                raise RuntimeError(f"{MAX_CONSECUTIVE_FAILURES} API failures in a row; stopping") from error
            time.sleep(PAUSE_SECONDS)
            continue

        consecutive_failures = 0
        if file_used == file_before:
            print(f"   {label} skipped, copy still fresh")
            continue
        calls_made += 1
        print(f"   {label} fetched ({time.monotonic() - job_started:.1f} s)")
        time.sleep(PAUSE_SECONDS)
    return calls_made, failed_jobs


def main():
    run_started = time.monotonic()

    print("1. Extract")
    sites_file = extract_sites()
    extract_health_advice()
    window_start, window_end = daily_window(date.today())
    jobs = plan_jobs(load_measurement_periods(sites_file), window_start, window_end)
    print(f"   {len(jobs)} jobs: {window_start} to {window_end}")
    calls_made, failed_jobs = fetch_recent_readings(jobs)
    print(f"   done: {calls_made} API calls, {len(failed_jobs)} failed")

    print("\n2. Stage")
    stage_new_raw_files()
    stage_sites()
    stage_health_advice()

    print("\n3. Clean")
    clean_readings()

    print("\n4. Health index")
    index_readings()

    print("\n5. Load warehouse")
    load_warehouse()

    print("\n6. Marts")
    refresh_marts()

    print(f"\nDaily run took {(time.monotonic() - run_started) / 60:.1f} minutes")


if __name__ == "__main__":
    main()