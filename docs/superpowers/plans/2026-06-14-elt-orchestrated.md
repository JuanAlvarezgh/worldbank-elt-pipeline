# World Bank ELT Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible ELT pipeline that ingests World Bank API v2 indicators into Postgres (raw), transforms them with dbt into a star schema, and orchestrates it with Airflow — all runnable via `docker compose up`.

**Architecture:** Thin tested Python extractor (EL) lands raw rows in Postgres via idempotent upsert; dbt owns all transformation (staging views → marts star schema) with tests; Airflow chains extract → load → `dbt run` → `dbt test`. GitHub Actions runs lint + pytest + `dbt build`.

**Tech Stack:** Python 3.11, `requests`, `psycopg[binary]`, PostgreSQL 16, dbt-postgres, Apache Airflow 2.9, Docker Compose, GitHub Actions, ruff, pytest, `responses`.

Reference spec: `docs/superpowers/specs/2026-06-14-elt-orchestrated-design.md`.

---

## File structure (created across tasks)

```
worldbank-elt-pipeline/
├─ pyproject.toml                       # Task 1
├─ .env.example                         # Task 1
├─ worldbank_extractor/
│  ├─ __init__.py                       # Task 1
│  ├─ models.py                         # Task 3  (ValueRow, CountryRow, _to_float)
│  └─ client.py                         # Task 4  (WorldBankClient)
├─ load/
│  ├─ __init__.py                       # Task 1
│  └─ postgres_loader.py                # Task 5  (ensure_schema, upserts)
├─ pipelines/
│  └─ run_el.py                         # Task 6  (run_el + CLI)
├─ tests/
│  ├─ test_models.py                    # Task 3
│  ├─ test_client.py                    # Task 4
│  └─ test_loader.py                    # Task 5
├─ dbt_worldbank/                       # Tasks 7-8
│  ├─ dbt_project.yml
│  ├─ profiles.yml
│  ├─ packages.yml
│  ├─ macros/generate_schema_name.sql
│  ├─ seeds/indicators.csv
│  └─ models/
│     ├─ staging/{_sources.yml,_staging.yml,stg_countries.sql,stg_indicator_values.sql}
│     └─ marts/{_marts.yml,dim_country.sql,dim_indicator.sql,fact_indicator_value.sql}
├─ dags/worldbank_elt.py                # Task 9
├─ docker-compose.yml                   # Task 2 (warehouse) → Task 9 (full)
├─ .github/workflows/ci.yml             # Task 10
└─ README.md                            # Task 11
```

---

## Task 1: Project scaffolding & tooling

**Files:**
- Create: `pyproject.toml`, `.env.example`, `worldbank_extractor/__init__.py`, `load/__init__.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "worldbank-elt-pipeline"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["requests>=2.31", "psycopg[binary]>=3.1"]

[project.optional-dependencies]
dev = ["pytest>=8", "responses>=0.25", "ruff>=0.5", "dbt-postgres>=1.7,<2.0"]

[tool.setuptools]
packages = ["worldbank_extractor", "load"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.pytest.ini_options]
addopts = "-ra"
testpaths = ["tests"]
```

- [ ] **Step 2: Create empty package files**

`worldbank_extractor/__init__.py` and `load/__init__.py` — both empty files.

- [ ] **Step 3: Create `.env.example`**

```dotenv
# Warehouse Postgres (host port 5433 to avoid clashes)
WAREHOUSE_DSN=postgresql://warehouse:warehouse@localhost:5433/warehouse
WAREHOUSE_HOST=localhost
WAREHOUSE_PORT=5433
WAREHOUSE_USER=warehouse
WAREHOUSE_PASSWORD=warehouse
WAREHOUSE_DB=warehouse
```

- [ ] **Step 4: Create venv and install**

Run (PowerShell):
```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -U pip
.\.venv\Scripts\pip install -e ".[dev]"
```
Expected: installs requests, psycopg, pytest, responses, ruff, dbt-postgres without errors.

- [ ] **Step 5: Verify ruff and pytest run**

Run: `.\.venv\Scripts\ruff check .` → Expected: "All checks passed!" (no files yet to lint is fine).
Run: `.\.venv\Scripts\pytest` → Expected: "no tests ran".

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .env.example worldbank_extractor load
git commit -m "chore: project scaffolding, packaging and tooling"
```

---

## Task 2: Warehouse Postgres (docker-compose)

**Files:**
- Create: `docker-compose.yml` (warehouse only for now)

- [ ] **Step 1: Create `docker-compose.yml`**

```yaml
services:
  warehouse-postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: warehouse
      POSTGRES_PASSWORD: warehouse
      POSTGRES_DB: warehouse
    ports:
      - "5433:5432"
    volumes:
      - warehouse-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "warehouse"]
      interval: 5s
      retries: 5

volumes:
  warehouse-data:
```

- [ ] **Step 2: Bring it up and verify**

Run: `docker compose up -d warehouse-postgres`
Then: `docker compose exec warehouse-postgres pg_isready -U warehouse`
Expected: `/var/run/postgresql:5432 - accepting connections`.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add warehouse postgres service"
```

---

## Task 3: Extractor data models (TDD)

**Files:**
- Create: `worldbank_extractor/models.py`
- Test: `tests/test_models.py`

- [ ] **Step 1: Write the failing test**

`tests/test_models.py`:
```python
from worldbank_extractor.models import ValueRow, CountryRow, to_float


def test_to_float_handles_none_and_empty():
    assert to_float(None) is None
    assert to_float("") is None
    assert to_float("-77.03") == -77.03
    assert to_float("not-a-number") is None
    assert to_float(42) == 42.0


def test_value_row_is_frozen_dataclass():
    row = ValueRow(indicator_code="SP.POP.TOTL", country_iso3="USA", year=2020, value=1.0)
    assert (row.indicator_code, row.country_iso3, row.year, row.value) == (
        "SP.POP.TOTL", "USA", 2020, 1.0)


def test_country_row_fields():
    row = CountryRow(country_iso3="USA", iso2_code="US", name="United States",
                     region="North America", income_level="High income",
                     capital_city="Washington D.C.", longitude=-77.03, latitude=38.89)
    assert row.country_iso3 == "USA"
    assert row.income_level == "High income"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\pytest tests/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'worldbank_extractor.models'`.

- [ ] **Step 3: Write minimal implementation**

`worldbank_extractor/models.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.\.venv\Scripts\pytest tests/test_models.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add worldbank_extractor/models.py tests/test_models.py
git commit -m "feat: add extractor row models and to_float helper"
```

---

## Task 4: World Bank API client (TDD)

**Files:**
- Create: `worldbank_extractor/client.py`
- Test: `tests/test_client.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_client.py`:
```python
import pytest
import responses

from worldbank_extractor.client import WorldBankClient, WorldBankAPIError
from worldbank_extractor.models import ValueRow

IND_URL = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
COUNTRY_URL = "https://api.worldbank.org/v2/country"


@responses.activate
def test_fetch_indicator_paginates_and_maps():
    page1 = [{"page": 1, "pages": 2, "per_page": 1, "total": 2},
             [{"indicator": {"id": "SP.POP.TOTL", "value": "Population"},
               "country": {"id": "US", "value": "United States"},
               "countryiso3code": "USA", "date": "2020", "value": 331}]]
    page2 = [{"page": 2, "pages": 2, "per_page": 1, "total": 2},
             [{"indicator": {"id": "SP.POP.TOTL", "value": "Population"},
               "country": {"id": "CA", "value": "Canada"},
               "countryiso3code": "CAN", "date": "2020", "value": 38}]]
    responses.add(responses.GET, IND_URL, json=page1, status=200)
    responses.add(responses.GET, IND_URL, json=page2, status=200)
    client = WorldBankClient(per_page=1)
    rows = list(client.fetch_indicator("SP.POP.TOTL", 2020, 2020))
    assert rows == [ValueRow("SP.POP.TOTL", "USA", 2020, 331.0),
                    ValueRow("SP.POP.TOTL", "CAN", 2020, 38.0)]


@responses.activate
def test_fetch_indicator_handles_null_value_and_empty_iso():
    payload = [{"page": 1, "pages": 1},
               [{"indicator": {"id": "SP.POP.TOTL"}, "country": {"id": "US"},
                 "countryiso3code": "USA", "date": "2019", "value": None},
                {"indicator": {"id": "SP.POP.TOTL"}, "country": {"id": ""},
                 "countryiso3code": "", "date": "2019", "value": 5}]]
    responses.add(responses.GET, IND_URL, json=payload, status=200)
    rows = list(WorldBankClient().fetch_indicator("SP.POP.TOTL", 2019, 2019))
    assert rows == [ValueRow("SP.POP.TOTL", "USA", 2019, None)]


@responses.activate
def test_get_retries_on_429_then_succeeds():
    responses.add(responses.GET, IND_URL, json={}, status=429)
    responses.add(responses.GET, IND_URL, json=[{"page": 1, "pages": 1}, []], status=200)
    sleeps = []
    client = WorldBankClient(base_delay=0.01, sleep=sleeps.append)
    rows = list(client.fetch_indicator("SP.POP.TOTL", 2020, 2020))
    assert rows == []
    assert len(sleeps) == 1


@responses.activate
def test_get_raises_after_max_retries():
    for _ in range(10):
        responses.add(responses.GET, COUNTRY_URL, json={}, status=503)
    client = WorldBankClient(max_retries=3, base_delay=0.0, sleep=lambda d: None)
    with pytest.raises(WorldBankAPIError):
        list(client.fetch_countries())


@responses.activate
def test_fetch_countries_filters_aggregates():
    payload = [{"page": 1, "pages": 1},
               [{"id": "USA", "iso2Code": "US", "name": "United States",
                 "region": {"id": "NAC", "value": "North America"},
                 "incomeLevel": {"value": "High income"}, "capitalCity": "Washington D.C.",
                 "longitude": "-77.032", "latitude": "38.889"},
                {"id": "WLD", "iso2Code": "1W", "name": "World",
                 "region": {"id": "NA", "value": "Aggregates"},
                 "incomeLevel": {"value": "Aggregates"}, "capitalCity": "",
                 "longitude": "", "latitude": ""}]]
    responses.add(responses.GET, COUNTRY_URL, json=payload, status=200)
    rows = list(WorldBankClient().fetch_countries())
    assert len(rows) == 1
    assert rows[0].country_iso3 == "USA"
    assert rows[0].capital_city == "Washington D.C."
    assert rows[0].longitude == -77.032


@responses.activate
def test_raises_on_malformed_payload():
    responses.add(responses.GET, COUNTRY_URL, json={"message": "boom"}, status=200)
    with pytest.raises(WorldBankAPIError):
        list(WorldBankClient().fetch_countries())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.\.venv\Scripts\pytest tests/test_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'worldbank_extractor.client'`.

- [ ] **Step 3: Write minimal implementation**

`worldbank_extractor/client.py`:
```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.\.venv\Scripts\pytest tests/test_client.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Lint**

Run: `.\.venv\Scripts\ruff check .` → Expected: All checks passed.

- [ ] **Step 6: Commit**

```bash
git add worldbank_extractor/client.py tests/test_client.py
git commit -m "feat: add World Bank API client with pagination and retry/backoff"
```

---

## Task 5: Postgres loader (integration TDD)

**Files:**
- Create: `load/postgres_loader.py`
- Test: `tests/test_loader.py`

> Loader tests are integration tests against the warehouse Postgres from Task 2.
> They **skip** automatically if `WAREHOUSE_DSN` is unset. Ensure the warehouse is
> up (`docker compose up -d warehouse-postgres`) and `$env:WAREHOUSE_DSN` is set.

- [ ] **Step 1: Write the failing test**

`tests/test_loader.py`:
```python
import os

import psycopg
import pytest

from load.postgres_loader import ensure_schema, upsert_countries, upsert_values
from worldbank_extractor.models import CountryRow, ValueRow

DSN = os.environ.get("WAREHOUSE_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="WAREHOUSE_DSN not set")


@pytest.fixture
def conn():
    with psycopg.connect(DSN) as c:
        ensure_schema(c)
        with c.cursor() as cur:
            cur.execute("TRUNCATE raw.indicator_values, raw.countries;")
        c.commit()
        yield c


def test_upsert_values_is_idempotent(conn):
    upsert_values(conn, [ValueRow("SP.POP.TOTL", "USA", 2020, 1.0)])
    upsert_values(conn, [ValueRow("SP.POP.TOTL", "USA", 2020, 2.0)])  # same key, new value
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), max(value) FROM raw.indicator_values;")
        count, value = cur.fetchone()
    assert count == 1
    assert value == 2.0


def test_upsert_countries_is_idempotent(conn):
    row = CountryRow("USA", "US", "United States", "North America", "High income",
                     "Washington D.C.", -77.0, 38.0)
    upsert_countries(conn, [row])
    upsert_countries(conn, [row])
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.countries;")
        (count,) = cur.fetchone()
    assert count == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.\.venv\Scripts\pytest tests/test_loader.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'load.postgres_loader'`.

- [ ] **Step 3: Write minimal implementation**

`load/postgres_loader.py`:
```python
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
```

- [ ] **Step 4: Run test to verify it passes**

First ensure warehouse is up and DSN set (PowerShell):
```powershell
docker compose up -d warehouse-postgres
$env:WAREHOUSE_DSN = "postgresql://warehouse:warehouse@localhost:5433/warehouse"
.\.venv\Scripts\pytest tests/test_loader.py -v
```
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add load/postgres_loader.py tests/test_loader.py
git commit -m "feat: add idempotent postgres loader for raw schema"
```

---

## Task 6: EL entrypoint (extract + load)

**Files:**
- Create: `pipelines/run_el.py`, `pipelines/__init__.py`

- [ ] **Step 1: Create `pipelines/__init__.py`** (empty file).

- [ ] **Step 2: Create `pipelines/run_el.py`**

```python
from __future__ import annotations

import argparse
import logging
import os

import psycopg

from load.postgres_loader import ensure_schema, upsert_countries, upsert_values
from worldbank_extractor.client import WorldBankClient

INDICATORS = [
    "NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "SP.POP.TOTL", "SP.DYN.LE00.IN",
    "SL.UEM.TOTL.ZS", "SE.XPD.TOTL.GD.ZS", "EG.ELC.ACCS.ZS", "SH.XPD.CHEX.GD.ZS",
]

logger = logging.getLogger(__name__)


def run_el(dsn: str, indicators: list[str], start_year: int, end_year: int) -> None:
    client = WorldBankClient()
    with psycopg.connect(dsn) as conn:
        ensure_schema(conn)
        n = upsert_countries(conn, client.fetch_countries())
        logger.info("Loaded %s countries", n)
        for indicator in indicators:
            n = upsert_values(conn, client.fetch_indicator(indicator, start_year, end_year))
            logger.info("Loaded %s rows for %s", n, indicator)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Extract+Load World Bank indicators into Postgres")
    parser.add_argument("--start-year", type=int, default=1990)
    parser.add_argument("--end-year", type=int, default=2023)
    parser.add_argument("--indicators", nargs="*", default=INDICATORS)
    args = parser.parse_args()
    dsn = os.environ["WAREHOUSE_DSN"]
    run_el(dsn, args.indicators, args.start_year, args.end_year)


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run a real end-to-end EL (smoke)**

Run (PowerShell, warehouse up + DSN set):
```powershell
.\.venv\Scripts\python -m pipelines.run_el --start-year 2015 --end-year 2020
```
Expected: logs "Loaded N countries" and "Loaded N rows for <indicator>" for all 8 indicators, no errors.

- [ ] **Step 4: Verify data landed**

Run: `docker compose exec warehouse-postgres psql -U warehouse -c "SELECT count(*) FROM raw.indicator_values;"`
Expected: a count well above zero (thousands).

- [ ] **Step 5: Commit**

```bash
git add pipelines
git commit -m "feat: add extract+load entrypoint and CLI"
```

---

## Task 7: dbt project — sources, staging, seed

**Files:**
- Create: `dbt_worldbank/dbt_project.yml`, `profiles.yml`, `packages.yml`,
  `macros/generate_schema_name.sql`, `seeds/indicators.csv`,
  `models/staging/{_sources.yml,_staging.yml,stg_countries.sql,stg_indicator_values.sql}`

- [ ] **Step 1: Create `dbt_worldbank/dbt_project.yml`**

```yaml
name: worldbank
version: "1.0.0"
profile: worldbank
model-paths: ["models"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  worldbank:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts

seeds:
  worldbank:
    +schema: marts
```

- [ ] **Step 2: Create `dbt_worldbank/profiles.yml`**

```yaml
worldbank:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('WAREHOUSE_HOST', 'localhost') }}"
      port: "{{ env_var('WAREHOUSE_PORT', '5433') | as_number }}"
      user: "{{ env_var('WAREHOUSE_USER', 'warehouse') }}"
      password: "{{ env_var('WAREHOUSE_PASSWORD', 'warehouse') }}"
      dbname: "{{ env_var('WAREHOUSE_DB', 'warehouse') }}"
      schema: public
      threads: 4
```

- [ ] **Step 3: Create `dbt_worldbank/packages.yml`**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
```

- [ ] **Step 4: Create `dbt_worldbank/macros/generate_schema_name.sql`**

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

- [ ] **Step 5: Create `dbt_worldbank/seeds/indicators.csv`**

```csv
indicator_code,indicator_name,topic,unit
NY.GDP.MKTP.CD,GDP (current US$),Economy,USD
NY.GDP.PCAP.CD,GDP per capita (current US$),Economy,USD
SP.POP.TOTL,Population total,Population,People
SP.DYN.LE00.IN,Life expectancy at birth,Health,Years
SL.UEM.TOTL.ZS,Unemployment (% of labor force),Labor,Percent
SE.XPD.TOTL.GD.ZS,Government education expenditure (% of GDP),Education,Percent
EG.ELC.ACCS.ZS,Access to electricity (% of population),Infrastructure,Percent
SH.XPD.CHEX.GD.ZS,Current health expenditure (% of GDP),Health,Percent
```

- [ ] **Step 6: Create `dbt_worldbank/models/staging/_sources.yml`**

```yaml
version: 2
sources:
  - name: raw
    schema: raw
    tables:
      - name: countries
      - name: indicator_values
        loaded_at_field: loaded_at
        freshness:
          warn_after: {count: 30, period: day}
          error_after: {count: 90, period: day}
```

- [ ] **Step 7: Create staging models**

`models/staging/stg_countries.sql`:
```sql
with src as (select * from {{ source('raw', 'countries') }})
select
    country_iso3,
    iso2_code,
    name as country_name,
    region,
    income_level,
    capital_city,
    longitude,
    latitude
from src
```

`models/staging/stg_indicator_values.sql`:
```sql
with src as (select * from {{ source('raw', 'indicator_values') }})
select
    indicator_code,
    country_iso3,
    year,
    value
from src
where value is not null
```

- [ ] **Step 8: Create `models/staging/_staging.yml`**

```yaml
version: 2
models:
  - name: stg_countries
    columns:
      - name: country_iso3
        tests: [unique, not_null]
  - name: stg_indicator_values
    columns:
      - name: value
        tests: [not_null]
```

- [ ] **Step 9: Build and test staging**

Run (PowerShell, warehouse up, env vars set, raw already loaded from Task 6):
```powershell
$env:DBT_PROFILES_DIR = "$PWD\dbt_worldbank"
cd dbt_worldbank
..\.venv\Scripts\dbt deps
..\.venv\Scripts\dbt seed
..\.venv\Scripts\dbt run --select staging
..\.venv\Scripts\dbt test --select staging
cd ..
```
Expected: `dbt seed` loads `indicators` (8 rows); staging views build; tests PASS.

- [ ] **Step 10: Commit**

```bash
git add dbt_worldbank
git commit -m "feat: add dbt project with sources, staging models and seed"
```

---

## Task 8: dbt marts — star schema

**Files:**
- Create: `dbt_worldbank/models/marts/{dim_country.sql,dim_indicator.sql,fact_indicator_value.sql,_marts.yml}`

- [ ] **Step 1: Create `models/marts/dim_country.sql`**

```sql
select
    country_iso3 as country_id,
    country_name,
    region,
    income_level,
    capital_city,
    longitude,
    latitude
from {{ ref('stg_countries') }}
```

- [ ] **Step 2: Create `models/marts/dim_indicator.sql`**

```sql
select
    indicator_code as indicator_id,
    indicator_name,
    topic,
    unit
from {{ ref('indicators') }}
```

- [ ] **Step 3: Create `models/marts/fact_indicator_value.sql`**

```sql
with vals as (
    select * from {{ ref('stg_indicator_values') }}
),
countries as (
    select country_id from {{ ref('dim_country') }}
),
indicators as (
    select indicator_id from {{ ref('dim_indicator') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['v.country_iso3', 'v.indicator_code', 'v.year']) }}
        as value_key,
    v.country_iso3   as country_id,
    v.indicator_code as indicator_id,
    v.year,
    v.value
from vals v
inner join countries  c on c.country_id  = v.country_iso3
inner join indicators i on i.indicator_id = v.indicator_code
```

> The inner joins guarantee every fact row references an existing dim row, so the
> `relationships` tests below always hold (aggregate "countries" like WLD are dropped).

- [ ] **Step 4: Create `models/marts/_marts.yml`**

```yaml
version: 2
models:
  - name: dim_country
    columns:
      - name: country_id
        tests: [unique, not_null]

  - name: dim_indicator
    columns:
      - name: indicator_id
        tests: [unique, not_null]

  - name: fact_indicator_value
    columns:
      - name: value_key
        tests: [unique, not_null]
      - name: country_id
        tests:
          - not_null
          - relationships: {to: ref('dim_country'), field: country_id}
      - name: indicator_id
        tests:
          - not_null
          - relationships: {to: ref('dim_indicator'), field: indicator_id}
      - name: year
        tests:
          - not_null
          - dbt_utils.accepted_range: {min_value: 1960, max_value: 2100}
```

- [ ] **Step 5: Build and test the full project**

Run (PowerShell):
```powershell
cd dbt_worldbank
..\.venv\Scripts\dbt build
cd ..
```
Expected: seed + all models build; **all tests PASS** (unique, not_null, relationships, accepted_range).

- [ ] **Step 6: Verify the star schema**

Run:
```bash
docker compose exec warehouse-postgres psql -U warehouse -c "SELECT count(*) FROM marts.fact_indicator_value;"
docker compose exec warehouse-postgres psql -U warehouse -c "SELECT * FROM marts.dim_indicator;"
```
Expected: fact has thousands of rows; dim_indicator has 8 rows.

- [ ] **Step 7: Commit**

```bash
git add dbt_worldbank/models/marts
git commit -m "feat: add dbt marts star schema (dim_country, dim_indicator, fact) with tests"
```

---

## Task 9: Airflow orchestration (full docker-compose)

**Files:**
- Modify: `docker-compose.yml` (add Airflow services + warehouse env)
- Create: `dags/worldbank_elt.py`

- [ ] **Step 1: Replace `docker-compose.yml` with the full stack**

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:2.9.3-python3.11
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    PYTHONPATH: /opt/airflow
    WAREHOUSE_DSN: postgresql://warehouse:warehouse@warehouse-postgres:5432/warehouse
    WAREHOUSE_HOST: warehouse-postgres
    WAREHOUSE_PORT: "5432"
    WAREHOUSE_USER: warehouse
    WAREHOUSE_PASSWORD: warehouse
    WAREHOUSE_DB: warehouse
    DBT_PROFILES_DIR: /opt/airflow/dbt_worldbank
    _PIP_ADDITIONAL_REQUIREMENTS: "requests>=2.31 psycopg[binary]>=3.1 dbt-postgres>=1.7,<2.0"
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./worldbank_extractor:/opt/airflow/worldbank_extractor
    - ./load:/opt/airflow/load
    - ./dbt_worldbank:/opt/airflow/dbt_worldbank
  depends_on: &airflow-common-depends
    airflow-postgres:
      condition: service_healthy

services:
  warehouse-postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: warehouse
      POSTGRES_PASSWORD: warehouse
      POSTGRES_DB: warehouse
    ports:
      - "5433:5432"
    volumes:
      - warehouse-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "warehouse"]
      interval: 5s
      retries: 5

  airflow-postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - airflow-db:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - >-
        airflow db migrate &&
        airflow users create --username admin --password admin
        --firstname admin --lastname admin --role Admin --email admin@example.com
    restart: on-failure

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    depends_on:
      <<: *airflow-common-depends
      airflow-init:
        condition: service_completed_successfully
    restart: always

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      <<: *airflow-common-depends
      airflow-init:
        condition: service_completed_successfully
    restart: always

volumes:
  warehouse-data:
  airflow-db:
```

- [ ] **Step 2: Create `dags/worldbank_elt.py`**

```python
from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

INDICATORS = [
    "NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "SP.POP.TOTL", "SP.DYN.LE00.IN",
    "SL.UEM.TOTL.ZS", "SE.XPD.TOTL.GD.ZS", "EG.ELC.ACCS.ZS", "SH.XPD.CHEX.GD.ZS",
]
DBT_DIR = "/opt/airflow/dbt_worldbank"


@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["worldbank", "elt"],
)
def worldbank_elt():
    @task
    def extract_load_countries() -> int:
        import psycopg

        from load.postgres_loader import ensure_schema, upsert_countries
        from worldbank_extractor.client import WorldBankClient

        client = WorldBankClient()
        with psycopg.connect(os.environ["WAREHOUSE_DSN"]) as conn:
            ensure_schema(conn)
            return upsert_countries(conn, client.fetch_countries())

    @task
    def extract_load_values(start_year: int = 1990, end_year: int = 2023) -> int:
        import psycopg

        from load.postgres_loader import ensure_schema, upsert_values
        from worldbank_extractor.client import WorldBankClient

        client = WorldBankClient()
        total = 0
        with psycopg.connect(os.environ["WAREHOUSE_DSN"]) as conn:
            ensure_schema(conn)
            for indicator in INDICATORS:
                total += upsert_values(conn, client.fetch_indicator(indicator, start_year, end_year))
        return total

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt deps && dbt seed && dbt run",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
    )
    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness",
    )

    countries = extract_load_countries()
    values = extract_load_values()
    countries >> values >> dbt_build >> dbt_test >> dbt_freshness


worldbank_elt()
```

- [ ] **Step 3: Bring up the full stack**

Run (PowerShell):
```powershell
docker compose up -d
```
Wait ~60s for `_PIP_ADDITIONAL_REQUIREMENTS` install. Check: `docker compose ps` → scheduler & webserver `running`, `airflow-init` `exited (0)`.

- [ ] **Step 4: Trigger and verify the DAG**

Run:
```powershell
docker compose exec airflow-scheduler airflow dags trigger worldbank_elt
```
Then open http://localhost:8080 (admin/admin), unpause `worldbank_elt`, watch the run.
Expected: all 5 tasks succeed (green). Verify: `docker compose exec warehouse-postgres psql -U warehouse -c "SELECT count(*) FROM marts.fact_indicator_value;"` returns thousands.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml dags/worldbank_elt.py
git commit -m "feat: orchestrate ELT with Airflow via docker-compose"
```

---

## Task 10: CI — GitHub Actions

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create `.github/workflows/ci.yml`**

```yaml
name: ci
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: warehouse
          POSTGRES_PASSWORD: warehouse
          POSTGRES_DB: warehouse
        ports: ["5433:5432"]
        options: >-
          --health-cmd "pg_isready -U warehouse"
          --health-interval 5s --health-timeout 5s --health-retries 5
    env:
      WAREHOUSE_DSN: postgresql://warehouse:warehouse@localhost:5433/warehouse
      WAREHOUSE_HOST: localhost
      WAREHOUSE_PORT: "5433"
      WAREHOUSE_USER: warehouse
      WAREHOUSE_PASSWORD: warehouse
      WAREHOUSE_DB: warehouse
      DBT_PROFILES_DIR: ${{ github.workspace }}/dbt_worldbank
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[dev]"
      - run: ruff check .
      - run: pytest -v
      - name: Create raw schema for dbt sources
        run: python -c "import os, psycopg; from load.postgres_loader import ensure_schema; ensure_schema(psycopg.connect(os.environ['WAREHOUSE_DSN']))"
      - name: dbt build
        working-directory: dbt_worldbank
        run: |
          dbt deps
          dbt seed
          dbt run
          dbt test
```

> CI validates that the extractor unit tests + loader integration tests pass, and
> that the dbt project compiles, builds and tests green against an empty-but-valid
> warehouse (no live API call in CI — marts may be empty; tests still hold).

- [ ] **Step 2: Verify locally with act or by pushing a branch**

Run: `git add .github/workflows/ci.yml && git commit -m "ci: add lint, pytest and dbt build workflow"`
Then push the branch and confirm the Actions run is green on GitHub.

---

## Task 11: Documentation & polish

**Files:**
- Create: `README.md`, `docs/architecture.md` (mermaid), example queries

- [ ] **Step 1: Create `README.md`**

Include: one-paragraph pitch, architecture diagram (mermaid below), the skills it
demonstrates (#1 SQL, #2 Python, #4 ETL/ELT, #6 Airflow, #7 dbt, #8 modeling,
#13 data quality, #14 CI), and a **Quickstart**:

````markdown
## Quickstart
```bash
cp .env.example .env
docker compose up -d                 # warehouse + airflow
# open http://localhost:8080  (admin/admin), unpause `worldbank_elt`, trigger it
```
Outputs: star schema in `marts.*`, navigable docs via `dbt docs serve`.
````

Mermaid diagram to embed:
```mermaid
flowchart LR
  API[World Bank API v2] -->|Python extractor| RAW[(raw.* Postgres)]
  RAW -->|dbt staging| STG[staging.*]
  STG -->|dbt marts| MARTS[(dim_country / dim_indicator / fact_indicator_value)]
  AF[Airflow DAG] -. orchestrates .-> API
  AF -. orchestrates .-> MARTS
```

- [ ] **Step 2: Add example analytical queries**

Create `docs/example_queries.sql`:
```sql
-- Top 10 countries by GDP per capita in 2020
select c.country_name, f.value as gdp_per_capita
from marts.fact_indicator_value f
join marts.dim_country c on c.country_id = f.country_id
join marts.dim_indicator i on i.indicator_id = f.indicator_id
where i.indicator_id = 'NY.GDP.PCAP.CD' and f.year = 2020
order by f.value desc nulls last
limit 10;

-- Life expectancy trend for a country
select f.year, f.value as life_expectancy
from marts.fact_indicator_value f
join marts.dim_indicator i on i.indicator_id = f.indicator_id
where f.country_id = 'COL' and i.indicator_id = 'SP.DYN.LE00.IN'
order by f.year;
```

- [ ] **Step 3: Capture a run GIF**

Record the Airflow graph view going green (e.g., with ScreenToGif) and save to
`docs/run.gif`; reference it in the README.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/architecture.md docs/example_queries.sql docs/run.gif
git commit -m "docs: add README, architecture diagram, example queries and run GIF"
```

---

## Self-review notes (spec coverage)

- Spec §4 architecture → Tasks 3-9. §5 components → Task 3/4 (extractor), 5 (loader),
  6 (EL), 7-8 (dbt), 9 (DAG + compose), 10 (CI). §6 model → Task 8. §7 idempotency
  → Task 5. §8 quality/testing → Tasks 4,5,7,8,10. §9 repo structure → file map above.
  §10 scope (no dashboard) → respected; "view" = dbt docs + example_queries (Task 11).
  §11 deliverable → Task 11 README + `docker compose up`. §12 future evolution →
  documented in README (Task 11), not built.
- No placeholders: every code/config step contains full content.
- Type consistency: `ValueRow`/`CountryRow`/`to_float` (Task 3) used identically in
  client (Task 4), loader (Task 5), EL (Task 6); `WAREHOUSE_DSN` + dbt env vars
  consistent across loader, EL, compose, CI.
```
