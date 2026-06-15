import os

import psycopg
import pytest

from load.postgres_loader import asegurar_esquema, upsert_paises, upsert_valores
from worldbank_extractor.models import FilaPais, FilaValor

DSN = os.environ.get("WAREHOUSE_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="WAREHOUSE_DSN no configurado")


@pytest.fixture
def conn():
    with psycopg.connect(DSN) as c:
        asegurar_esquema(c)
        with c.cursor() as cur:
            cur.execute("TRUNCATE raw.valores_indicador, raw.paises;")
        c.commit()
        yield c


def test_upsert_valores_es_idempotente(conn):
    upsert_valores(conn, [FilaValor("SP.POP.TOTL", "USA", 2020, 1.0)])
    upsert_valores(conn, [FilaValor("SP.POP.TOTL", "USA", 2020, 2.0)])  # misma clave, nuevo valor
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), max(valor) FROM raw.valores_indicador;")
        count, valor = cur.fetchone()
    assert count == 1
    assert valor == 2.0


def test_upsert_paises_es_idempotente(conn):
    fila = FilaPais("USA", "US", "United States", "North America", "High income",
                    "Washington D.C.", -77.0, 38.0)
    upsert_paises(conn, [fila])
    upsert_paises(conn, [fila])
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.paises;")
        (count,) = cur.fetchone()
    assert count == 1
