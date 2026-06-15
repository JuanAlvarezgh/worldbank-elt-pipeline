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
