from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

INDICADORES = [
    "NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "SP.POP.TOTL", "SP.DYN.LE00.IN",
    "SL.UEM.TOTL.ZS", "SE.XPD.TOTL.GD.ZS", "EG.ELC.ACCS.ZS", "SH.XPD.CHEX.GD.ZS",
]
DBT_DIR = "/opt/airflow/dbt_worldbank"


@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["worldbank", "elt"],
    # La API pública del Banco Mundial devuelve timeouts/5xx transitorios bajo carga;
    # los reintentos a nivel de tarea los absorben.
    default_args={"retries": 3, "retry_delay": timedelta(seconds=30)},
)
def worldbank_elt():
    @task
    def extraer_cargar_paises() -> int:
        import psycopg

        from load.postgres_loader import asegurar_esquema, upsert_paises
        from worldbank_extractor.client import ClienteBancoMundial

        cliente = ClienteBancoMundial()
        with psycopg.connect(os.environ["WAREHOUSE_DSN"]) as conn:
            asegurar_esquema(conn)
            return upsert_paises(conn, cliente.obtener_paises())

    @task
    def extraer_cargar_valores(anio_inicio: int = 1990) -> int:
        import psycopg
        from airflow.operators.python import get_current_context

        from load.postgres_loader import asegurar_esquema, upsert_valores
        from worldbank_extractor.client import ClienteBancoMundial

        # Ingesta hasta el año de la ejecución programada para mantener datos actualizados.
        anio_fin = get_current_context()["data_interval_end"].year
        cliente = ClienteBancoMundial()
        total = 0
        with psycopg.connect(os.environ["WAREHOUSE_DSN"]) as conn:
            asegurar_esquema(conn)
            for indicador in INDICADORES:
                total += upsert_valores(conn, cliente.obtener_indicador(indicador, anio_inicio, anio_fin))
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

    paises = extraer_cargar_paises()
    valores = extraer_cargar_valores()
    paises >> valores >> dbt_build >> dbt_test >> dbt_freshness


worldbank_elt()
