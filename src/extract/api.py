import json

import requests

BASE_URL = "https://api.erg.ic.ac.uk/AirQuality"
TIMEOUT_SECONDS = 120
TIMESTAMP_FORMAT = "%Y-%m-%dT%H%M%SZ"


def fetch(path, expect_json=True):
    response = requests.get(f"{BASE_URL}{path}", timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    if expect_json:
        json.loads(response.content)
    return response.content