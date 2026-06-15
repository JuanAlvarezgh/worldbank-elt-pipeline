from __future__ import annotations

from collections.abc import Iterable

import psycopg

from worldbank_extractor.models import CountryRow, ValueRow

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.countries (
    country_iso3 text PRIMARY KEY,
    iso2_code    text,
    name         text,
    region       text,
    income_level text,
    capital_city text,
    longitude    double precision,
    latitude     double precision,
    loaded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.indicator_values (
    indicator_code text NOT NULL,
    country_iso3   text NOT NULL,
    year           int  NOT NULL,
    value          double precision,
    loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (indicator_code, country_iso3, year)
);
"""

_UPSERT_COUNTRIES = """
INSERT INTO raw.countries
    (country_iso3, iso2_code, name, region, income_level, capital_city, longitude, latitude)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (country_iso3) DO UPDATE SET
    iso2_code = EXCLUDED.iso2_code, name = EXCLUDED.name, region = EXCLUDED.region,
    income_level = EXCLUDED.income_level, capital_city = EXCLUDED.capital_city,
    longitude = EXCLUDED.longitude, latitude = EXCLUDED.latitude, loaded_at = now();
"""

_UPSERT_VALUES = """
INSERT INTO raw.indicator_values (indicator_code, country_iso3, year, value)
VALUES (%s, %s, %s, %s)
ON CONFLICT (indicator_code, country_iso3, year) DO UPDATE SET
    value = EXCLUDED.value, loaded_at = now();
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def upsert_countries(conn: psycopg.Connection, rows: Iterable[CountryRow]) -> int:
    params = [(r.country_iso3, r.iso2_code, r.name, r.region, r.income_level,
               r.capital_city, r.longitude, r.latitude) for r in rows]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_COUNTRIES, params)
    conn.commit()
    return len(params)


def upsert_values(conn: psycopg.Connection, rows: Iterable[ValueRow]) -> int:
    params = [(r.indicator_code, r.country_iso3, r.year, r.value) for r in rows]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_VALUES, params)
    conn.commit()
    return len(params)
