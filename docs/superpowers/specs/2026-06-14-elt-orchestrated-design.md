# Design Spec — `worldbank-elt-pipeline` (codename: elt-orchestrated)

- **Date:** 2026-06-14
- **Status:** Approved (design); pending implementation plan
- **Author:** Juan
- **Portfolio context:** Project #2 of 6 in the Data Engineering / Data Analytics portfolio
  for remote-international DE/DA roles. Highest-ROI "modern data stack" piece.

---

## 1. Goal

Build a fully reproducible, end-to-end **ELT pipeline** that ingests public
indicators from the **World Bank API v2**, lands them raw in **PostgreSQL**,
transforms them with **dbt** into a **star schema**, and orchestrates the whole
flow with **Apache Airflow** — all runnable by a recruiter with a single
`docker compose up`.

The project must demonstrate both sides of the candidate's profile:
- **Software engineering:** a clean, tested Python extractor package (pagination,
  retry/backoff, rate-limit handling, idempotent loads).
- **Data engineering:** orchestration, dimensional modeling, data quality tests, CI.

## 2. Target skills demonstrated (from market research)

`#1 SQL` · `#2 Python` · `#4 ETL/ELT pipeline design` · `#6 Apache Airflow` ·
`#7 dbt` · `#8 Data modeling (star schema)` · `#13 Data quality` · `#14 Git/testing/CI`.

(Cloud warehouse #9 and BI #11 are intentionally deferred — see §10 Scope.)

## 3. Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Warehouse | **PostgreSQL** (Docker) | 100% reproducible, no cloud account/credit card; ELT pattern identical to cloud DW. |
| Source | **World Bank API v2** | Free, no API key, clean dimensional shape (country × indicator × year). |
| Orchestration | **Airflow** via official `docker-compose` | Max reproducibility (only needs Docker); shows real Airflow components. |
| Transform | **dbt** (`dbt-postgres`) | Industry-standard "modern data stack". |
| Languages | **Python 3.11 + SQL** | Matches near-universal job requirements. |
| Architecture | **Enfoque A — dbt-centric ELT** | Thin Python EL + dbt owns transformation; clean separation of concerns. |

## 4. Architecture & data flow

```
World Bank API v2 ──(Python extractor)──►  raw.*            [Postgres warehouse]
                                            │  (rows loaded as-is, idempotent upsert)
                                            ▼
                                 dbt  staging.*   (cleaned, typed, 1 row/measurement)
                                            ▼
                                 dbt  marts.*     (STAR SCHEMA)
                                  ├── dim_country
                                  ├── dim_indicator
                                  └── fact_indicator_value
                                          (country_id, indicator_id, year, value)

Orchestrated by Airflow DAG `worldbank_elt`:
  extract_countries → load_raw → extract_values → load_raw → dbt run → dbt test → dbt source freshness

Single-command bring-up:  docker compose up   (Airflow services + warehouse-postgres)
```

## 5. Components (each a unit with one purpose)

1. **`worldbank_extractor/`** — Python package, the "software" showcase.
   - API client with **pagination** (World Bank returns `[metadata, data]`; iterate `page`/`pages`).
   - **Retry with exponential backoff** on 5xx / 429; respects rate limits.
   - **Incremental by year window** (`date=YYYY:YYYY`), parameterizable.
   - Pure functions returning typed rows; no DB side effects (testable in isolation).
   - Public interface: `fetch_indicator(indicator, year_range) -> Iterable[ValueRow]`,
     `fetch_countries() -> Iterable[CountryRow]`.
2. **`load/`** — writes to `raw` schema in Postgres.
   - **Idempotent upsert** on natural keys (`ON CONFLICT ... DO UPDATE`):
     `(country_code, indicator_code, year)` for values; `country_code` for countries.
   - Reruns and backfills never duplicate.
3. **`dbt_worldbank/`** — dbt project.
   - `sources` (raw tables) with **freshness** checks.
   - `staging/` views: clean/type/rename, one row per measurement.
   - `marts/` tables: `dim_country`, `dim_indicator`, `fact_indicator_value`.
   - **Tests:** `unique`, `not_null`, `relationships`, `accepted_range`.
   - `dbt docs` generated (serves as the lightweight "view" of results).
4. **`dags/worldbank_elt.py`** — Airflow DAG (TaskFlow API).
   - Chains extract → load → `dbt run` → `dbt test` → freshness.
   - Supports **backfill** of historical years via Airflow's logical date / params.
   - Fails loudly if dbt tests fail.
5. **`docker-compose.yml`** — services: `airflow-scheduler`, `airflow-webserver`,
   `airflow-init`, `airflow-metadata-postgres`, and a separate **`warehouse-postgres`**.
6. **`.github/workflows/ci.yml`** — GitHub Actions: `ruff` lint + `pytest` +
   `dbt build` against a Postgres service container, on every push/PR.

## 6. Data model (star schema)

- **Grain of fact:** one row per **country × indicator × year**.
- **`dim_country`** (PK `country_id` = ISO3 code): `country_name`, `region`,
  `income_level`, `capital_city`, `longitude`, `latitude`. Source: `/v2/country`.
- **`dim_indicator`** (PK `indicator_id` = WB code): `indicator_name`, `topic`,
  `source_note`, `unit` (derived). Source: indicator catalog.
- **`fact_indicator_value`**: `country_id` (FK), `indicator_id` (FK), `year`,
  `value`, `loaded_at`. Composite natural key (country_id, indicator_id, year).

**Seed indicators (5–8; exact codes verified against `/v2/indicator` at build time):**
- `NY.GDP.MKTP.CD` — GDP (current US$)
- `NY.GDP.PCAP.CD` — GDP per capita (current US$)
- `SP.POP.TOTL` — Population, total
- `SP.DYN.LE00.IN` — Life expectancy at birth
- `SL.UEM.TOTL.ZS` — Unemployment (% of labor force)
- `SE.XPD.TOTL.GD.ZS` — Government education expenditure (% of GDP)
- `EG.ELC.ACCS.ZS` — Access to electricity (% of population)
- `SH.XPD.CHEX.GD.ZS` — Current health expenditure (% of GDP)

## 7. Error handling & idempotency

- Extractor retries 5xx/429 with exponential backoff; raises a clear error on
  unexpected schema (e.g., missing `pages` metadata) instead of silently passing.
- Structured logging at extract/load boundaries.
- Idempotent upsert by natural key → safe reruns and year backfills.

## 8. Data quality & testing

- **pytest** on the extractor: mocked API responses covering pagination, backoff
  on 429/5xx, empty pages, and load upsert idempotency.
- **dbt tests** on staging + marts (`unique`, `not_null`, `relationships`,
  `accepted_range` e.g. year within [1960, current+1]).
- **Source freshness** check in the DAG.
- The DAG and CI both **fail** when any test fails.

## 9. Repo structure (target)

```
worldbank-elt-pipeline/
├─ worldbank_extractor/        # python package (extract)
├─ load/                       # raw loaders (idempotent upsert)
├─ dags/worldbank_elt.py       # airflow DAG
├─ dbt_worldbank/              # dbt project (staging + marts + tests)
├─ tests/                      # pytest
├─ docker-compose.yml          # airflow + warehouse-postgres
├─ .github/workflows/ci.yml    # lint + tests + dbt build
├─ .env.example                # connection/config template
├─ docs/                       # this spec + architecture diagram + run GIF
└─ README.md                   # architecture, decisions, skills, how-to-run
```

## 10. Scope (in / out) — to fit 2–4 weeks

**In:** tested extractor, idempotent raw load, dbt staging+marts+tests+docs,
Airflow DAG with backfill, docker-compose one-command bring-up, GitHub Actions CI,
README with architecture diagram + run GIF.

**Out (intentional):**
- Heavy BI dashboard → that is **Project #4**, which reuses this warehouse.
  Here the "view" is `dbt docs` + 1–2 example analytical queries.
- Cloud warehouse (BigQuery/Snowflake) → see Future evolution.
- Streaming / CDC → out of scope (that is Project #1).

## 11. Deliverable

Public GitHub repo (attributed solely to Juan) a recruiter can **clone and run with
`docker compose up`**: green DAG in the Airflow UI, populated star schema in Postgres,
navigable `dbt docs`. README documents architecture, design decisions, and the
mapped portfolio skills.

## 12. Future evolution (out of scope, documented for narrative)

- Parameterize warehouse target to also deploy on **BigQuery**.
- Swap the hand-rolled extractor for **dlt/Meltano** as an "EL tool" variant.
- Connect **Project #4** (Power BI / Tableau) on top of the marts.

## 13. Assumptions

- Docker Desktop available locally (confirmed) and started before `docker compose up`.
- Python 3.11 (confirmed). dbt installed in a project virtualenv.
- World Bank API remains free and key-less (its long-standing public terms).
