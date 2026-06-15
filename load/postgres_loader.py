from __future__ import annotations

from collections.abc import Iterable

import psycopg

from worldbank_extractor.models import FilaPais, FilaValor

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.paises (
    pais_iso3      text PRIMARY KEY,
    codigo_iso2    text,
    nombre         text,
    region         text,
    nivel_ingreso  text,
    ciudad_capital text,
    longitud       double precision,
    latitud        double precision,
    cargado_en     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.valores_indicador (
    codigo_indicador text NOT NULL,
    pais_iso3        text NOT NULL,
    anio             int  NOT NULL,
    valor            double precision,
    cargado_en       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (codigo_indicador, pais_iso3, anio)
);
"""

_UPSERT_PAISES = """
INSERT INTO raw.paises
    (pais_iso3, codigo_iso2, nombre, region, nivel_ingreso, ciudad_capital, longitud, latitud)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (pais_iso3) DO UPDATE SET
    codigo_iso2 = EXCLUDED.codigo_iso2, nombre = EXCLUDED.nombre, region = EXCLUDED.region,
    nivel_ingreso = EXCLUDED.nivel_ingreso, ciudad_capital = EXCLUDED.ciudad_capital,
    longitud = EXCLUDED.longitud, latitud = EXCLUDED.latitud, cargado_en = now();
"""

_UPSERT_VALORES = """
INSERT INTO raw.valores_indicador (codigo_indicador, pais_iso3, anio, valor)
VALUES (%s, %s, %s, %s)
ON CONFLICT (codigo_indicador, pais_iso3, anio) DO UPDATE SET
    valor = EXCLUDED.valor, cargado_en = now();
"""


def asegurar_esquema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def upsert_paises(conn: psycopg.Connection, filas: Iterable[FilaPais]) -> int:
    params = [(f.pais_iso3, f.codigo_iso2, f.nombre, f.region, f.nivel_ingreso,
               f.ciudad_capital, f.longitud, f.latitud) for f in filas]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_PAISES, params)
    conn.commit()
    return len(params)


def upsert_valores(conn: psycopg.Connection, filas: Iterable[FilaValor]) -> int:
    params = [(f.codigo_indicador, f.pais_iso3, f.anio, f.valor) for f in filas]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT_VALORES, params)
    conn.commit()
    return len(params)
