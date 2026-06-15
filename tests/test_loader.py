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
