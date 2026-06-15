from __future__ import annotations

import argparse
import logging
import os

import psycopg

from load.postgres_loader import asegurar_esquema, upsert_paises, upsert_valores
from worldbank_extractor.client import ClienteBancoMundial

INDICADORES = [
    "NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "SP.POP.TOTL", "SP.DYN.LE00.IN",
    "SL.UEM.TOTL.ZS", "SE.XPD.TOTL.GD.ZS", "EG.ELC.ACCS.ZS", "SH.XPD.CHEX.GD.ZS",
]

logger = logging.getLogger(__name__)


def ejecutar_el(dsn: str, indicadores: list[str], anio_inicio: int, anio_fin: int) -> None:
    cliente = ClienteBancoMundial()
    with psycopg.connect(dsn) as conn:
        asegurar_esquema(conn)
        n = upsert_paises(conn, cliente.obtener_paises())
        logger.info("Cargados %s paises", n)
        for indicador in indicadores:
            n = upsert_valores(conn, cliente.obtener_indicador(indicador, anio_inicio, anio_fin))
            logger.info("Cargadas %s filas para %s", n, indicador)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Extrae y carga indicadores del Banco Mundial en Postgres")
    parser.add_argument("--anio-inicio", type=int, default=1990)
    parser.add_argument("--anio-fin", type=int, default=2023)
    parser.add_argument("--indicadores", nargs="*", default=INDICADORES)
    args = parser.parse_args()
    dsn = os.environ["WAREHOUSE_DSN"]
    ejecutar_el(dsn, args.indicadores, args.anio_inicio, args.anio_fin)


if __name__ == "__main__":
    main()
