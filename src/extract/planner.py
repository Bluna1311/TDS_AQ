import json
from datetime import date, datetime

from src.transform.readings import as_list

TARGET_SPECIES = {"NO2", "O3", "PM25", "PM10"}


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


def plan_jobs(measurement_periods, start, end, months_per_chunk=1):
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