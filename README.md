# World Bank ELT Pipeline

An end-to-end, fully reproducible **ELT pipeline** that ingests public indicators from the
[World Bank API v2](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392), lands
them raw in **PostgreSQL**, transforms them with **dbt** into a **star schema**, and
orchestrates the whole flow with **Apache Airflow** — all runnable with a single
`docker compose up`.

The project demonstrates both sides of a data engineer's job: clean, tested **software**
(a Python extractor with pagination, retry/backoff and idempotent loads) and modern
**data engineering** (orchestration, dimensional modeling, data-quality tests, CI).

## Architecture

```mermaid
flowchart LR
  API["World Bank API v2"] -->|"Python extractor<br/>(pagination, retry, idempotent)"| RAW[("raw.* — Postgres")]
  RAW -->|"dbt staging (views)"| STG["staging.stg_countries<br/>staging.stg_indicator_values"]
  STG -->|"dbt marts (tables)"| MARTS[("Star schema<br/>dim_country · dim_indicator<br/>fact_indicator_value")]
  AF{{"Airflow DAG: worldbank_elt"}} -.->|orchestrates| API
  AF -.->|"dbt run / test / source freshness"| MARTS
```

**Flow:** `extract_load_countries → extract_load_values → dbt_build → dbt_test → dbt_source_freshness`

The fact table is at grain **country × indicator × year** and INNER JOINs to both dimensions,
which intentionally drops World Bank aggregate geographies (regions, income groups, "World")
and any indicator outside the curated seed — keeping referential integrity tests valid.

## Tech stack

| Layer | Tool |
|---|---|
| Languages | Python 3.11, SQL |
| Extract | `requests` (custom World Bank client) |
| Load | `psycopg` 3 → PostgreSQL 16 (idempotent `ON CONFLICT` upserts) |
| Transform | dbt 1.9 (`dbt-postgres`) + `dbt_utils` |
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| Infra | Docker Compose |
| Quality | pytest, ruff, dbt tests, source freshness |
| CI | GitHub Actions |

## Quickstart (full stack)

Requires Docker Desktop running.

```bash
cp .env.example .env
docker compose up -d            # warehouse-postgres + Airflow (scheduler, webserver, init)
```

Then open the Airflow UI at **http://localhost:8080** (user `admin` / pass `admin`), unpause
the **`worldbank_elt`** DAG and trigger it. When it goes green, the warehouse holds the
populated star schema. Inspect it:

```bash
docker compose exec warehouse-postgres psql -U warehouse -c "SELECT count(*) FROM marts.fact_indicator_value;"
```

Typical result: **~50,000** fact rows across **217 countries** and **8 indicators** (1990–2023).

Browse the data model docs:

```bash
# from the dbt project, with the warehouse up
cd dbt_worldbank && DBT_PROFILES_DIR=. dbt docs generate && DBT_PROFILES_DIR=. dbt docs serve
```

## Local development (without Airflow)

```bash
python -m venv .venv
.venv/Scripts/pip install -e ".[dev]"     # Windows; use .venv/bin/pip on macOS/Linux

# warehouse only
docker compose up -d warehouse-postgres
export WAREHOUSE_DSN=postgresql://warehouse:warehouse@localhost:5433/warehouse

# extract + load
python -m pipelines.run_el --start-year 1990 --end-year 2023

# transform + test
cd dbt_worldbank
export DBT_PROFILES_DIR=$PWD
dbt deps && dbt build        # seed + staging + marts + all tests
```

## Tests & quality

```bash
ruff check .
pytest -v
```

- **Unit tests** (`tests/test_models.py`, `tests/test_client.py`) cover the extractor:
  pagination, retry/backoff on 429/5xx, ISO/aggregate filtering, malformed-payload handling —
  all with mocked HTTP (no network).
- **Integration tests** (`tests/test_loader.py`) verify idempotent upserts against a live
  Postgres; they **skip** automatically unless `WAREHOUSE_DSN` is set.
- **dbt tests**: `unique`, `not_null`, `relationships` (FKs), and `accepted_range` (year),
  plus **source freshness**.

> **Note:** the loader integration tests `TRUNCATE raw.*` to isolate themselves, so they leave
> the warehouse with only their fixture rows. After running `pytest` locally, re-run
> `python -m pipelines.run_el ...` (or trigger the DAG) to repopulate `raw` before a full
> `dbt build`. In CI this is irrelevant — each run starts from a fresh ephemeral Postgres.

## Project structure

```
worldbank-elt-pipeline/
├─ worldbank_extractor/     # API client + typed row models (extract)
├─ load/                    # idempotent raw loaders (load)
├─ pipelines/run_el.py      # extract+load entrypoint & CLI
├─ dbt_worldbank/           # dbt project: sources, staging, marts, seed, tests
├─ dags/worldbank_elt.py    # Airflow DAG
├─ docker-compose.yml       # warehouse + Airflow
├─ .github/workflows/ci.yml # lint + tests + dbt build
└─ docs/                    # specs, plan, example queries
```

## Skills demonstrated

`SQL` · `Python` · `ETL/ELT pipeline design` · `Apache Airflow` · `dbt` ·
`dimensional modeling (star schema)` · `data quality & testing` · `Git / CI (GitHub Actions)` ·
`Docker`.

## Possible extensions

- Parameterize the warehouse target to also deploy on **BigQuery** / **Snowflake**.
- Swap the hand-rolled extractor for an EL tool (**dlt** / **Meltano**) as a variant.
- Build a BI layer (Power BI / Tableau) on top of the marts.
