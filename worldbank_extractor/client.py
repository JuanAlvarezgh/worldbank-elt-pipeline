from __future__ import annotations

import logging
import time
from collections.abc import Iterator

import requests

from worldbank_extractor.models import CountryRow, ValueRow, to_float

logger = logging.getLogger(__name__)
BASE_URL = "https://api.worldbank.org/v2"
RETRY_STATUS = {429, 500, 502, 503, 504}


class WorldBankAPIError(RuntimeError):
    """Raised on unrecoverable World Bank API responses."""


class WorldBankClient:
    def __init__(self, session: requests.Session | None = None, base_url: str = BASE_URL,
                 per_page: int = 1000, max_retries: int = 5, base_delay: float = 0.5,
                 sleep=time.sleep):
        self.session = session or requests.Session()
        self.base_url = base_url
        self.per_page = per_page
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._sleep = sleep

    def _get(self, url: str, params: dict) -> object:
        attempt = 0
        while True:
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code in RETRY_STATUS:
                if attempt >= self.max_retries:
                    raise WorldBankAPIError(f"Max retries exceeded for {url}: {resp.status_code}")
                delay = self.base_delay * (2 ** attempt)
                logger.warning("WB API %s -> %s, retrying in %.2fs", url, resp.status_code, delay)
                self._sleep(delay)
                attempt += 1
                continue
            if resp.status_code != 200:
                raise WorldBankAPIError(f"Unexpected status {resp.status_code} for {url}")
            return resp.json()

    def _paged(self, path: str, extra_params: dict | None = None) -> Iterator[dict]:
        page = 1
        while True:
            params = {"format": "json", "per_page": self.per_page, "page": page}
            if extra_params:
                params.update(extra_params)
            payload = self._get(f"{self.base_url}/{path}", params)
            if not isinstance(payload, list) or len(payload) < 2:
                raise WorldBankAPIError(f"Malformed payload for {path}: {payload!r}")
            meta, data = payload[0], payload[1]
            if not data:
                return
            yield from data
            if page >= int(meta.get("pages", 1)):
                return
            page += 1

    def fetch_countries(self) -> Iterator[CountryRow]:
        for row in self._paged("country"):
            region = (row.get("region") or {}).get("value")
            if region == "Aggregates":
                continue
            yield CountryRow(
                country_iso3=row["id"],
                iso2_code=row.get("iso2Code"),
                name=row.get("name"),
                region=region,
                income_level=(row.get("incomeLevel") or {}).get("value"),
                capital_city=(row.get("capitalCity") or None),
                longitude=to_float(row.get("longitude")),
                latitude=to_float(row.get("latitude")),
            )

    def fetch_indicator(self, indicator: str, start_year: int, end_year: int) -> Iterator[ValueRow]:
        path = f"country/all/indicator/{indicator}"
        for row in self._paged(path, {"date": f"{start_year}:{end_year}"}):
            iso3 = row.get("countryiso3code") or (row.get("country") or {}).get("id")
            if not iso3 or len(iso3) != 3:
                continue
            yield ValueRow(
                indicator_code=row["indicator"]["id"],
                country_iso3=iso3,
                year=int(row["date"]),
                value=to_float(row.get("value")),
            )
