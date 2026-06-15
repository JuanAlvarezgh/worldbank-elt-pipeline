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
    def extract_load_values(start_year: int = 1990) -> int:
        import psycopg
        from airflow.operators.python import get_current_context

        from load.postgres_loader import ensure_schema, upsert_values
        from worldbank_extractor.client import WorldBankClient

        # Ingest up to the year of the scheduled run so the pipeline keeps current.
        end_year = get_current_context()["data_interval_end"].year
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
