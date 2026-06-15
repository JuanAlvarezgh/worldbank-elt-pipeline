from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValueRow:
    indicator_code: str
    country_iso3: str
    year: int
    value: float | None


@dataclass(frozen=True)
class CountryRow:
    country_iso3: str
    iso2_code: str | None
    name: str | None
    region: str | None
    income_level: str | None
    capital_city: str | None
    longitude: float | None
    latitude: float | None


def to_float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
