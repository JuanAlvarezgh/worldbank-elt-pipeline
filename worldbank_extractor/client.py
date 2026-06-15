from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from typing import Any

import requests

from worldbank_extractor.models import FilaPais, FilaValor, a_float

logger = logging.getLogger(__name__)
BASE_URL = "https://api.worldbank.org/v2"
ESTADOS_REINTENTO = {429, 500, 502, 503, 504}


class ErrorAPIBancoMundial(RuntimeError):
    """Se lanza ante respuestas irrecuperables de la API del Banco Mundial."""


class ClienteBancoMundial:
    def __init__(self, sesion: requests.Session | None = None, base_url: str = BASE_URL,
                 por_pagina: int = 1000, max_reintentos: int = 5, retardo_base: float = 0.5,
                 dormir: Callable[[float], None] = time.sleep):
        self.sesion = sesion or requests.Session()
        self.base_url = base_url
        self.por_pagina = por_pagina
        self.max_reintentos = max_reintentos
        self.retardo_base = retardo_base
        self._dormir = dormir

    def cerrar(self) -> None:
        self.sesion.close()

    def __enter__(self) -> ClienteBancoMundial:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.cerrar()

    def _reintentar_o_fallar(self, intento: int, url: str, razon: str) -> None:
        """Espera antes del siguiente intento, o lanza si se agotaron los reintentos."""
        if intento >= self.max_reintentos:
            raise ErrorAPIBancoMundial(f"Reintentos agotados para {url}: {razon}")
        retardo = self.retardo_base * (2 ** intento)
        logger.warning("API BM %s -> %s, reintentando en %.2fs", url, razon, retardo)
        self._dormir(retardo)

    def _obtener(self, url: str, params: dict) -> Any:
        intento = 0
        while True:
            try:
                resp = self.sesion.get(url, params=params, timeout=30)
            except (requests.Timeout, requests.ConnectionError) as exc:
                # Fallos de red transitorios son reintentables, igual que 429/5xx.
                self._reintentar_o_fallar(intento, url, repr(exc))
                intento += 1
                continue
            if resp.status_code in ESTADOS_REINTENTO:
                self._reintentar_o_fallar(intento, url, str(resp.status_code))
                intento += 1
                continue
            if resp.status_code != 200:
                raise ErrorAPIBancoMundial(f"Estado inesperado {resp.status_code} para {url}")
            return resp.json()

    def _paginado(self, ruta: str, params_extra: dict | None = None) -> Iterator[dict]:
        pagina = 1
        while True:
            params = {"format": "json", "per_page": self.por_pagina, "page": pagina}
            if params_extra:
                params.update(params_extra)
            payload = self._obtener(f"{self.base_url}/{ruta}", params)
            if not isinstance(payload, list) or len(payload) < 2:
                raise ErrorAPIBancoMundial(f"Payload malformado para {ruta}: {payload!r}")
            meta, datos = payload[0], payload[1]
            if not datos:
                return
            yield from datos
            try:
                total_paginas = int(meta.get("pages", 1))
            except (TypeError, ValueError) as exc:
                raise ErrorAPIBancoMundial(
                    f"'pages' malformado en metadatos para {ruta}: {meta!r}"
                ) from exc
            if pagina >= total_paginas:
                return
            pagina += 1

    def obtener_paises(self) -> Iterator[FilaPais]:
        for fila in self._paginado("country"):
            region = (fila.get("region") or {}).get("value")
            if region == "Aggregates":
                continue
            yield FilaPais(
                pais_iso3=fila["id"],
                codigo_iso2=fila.get("iso2Code"),
                nombre=fila.get("name"),
                region=region,
                nivel_ingreso=(fila.get("incomeLevel") or {}).get("value"),
                ciudad_capital=(fila.get("capitalCity") or None),
                longitud=a_float(fila.get("longitude")),
                latitud=a_float(fila.get("latitude")),
            )

    def obtener_indicador(self, indicador: str, anio_inicio: int, anio_fin: int) -> Iterator[FilaValor]:
        ruta = f"country/all/indicator/{indicador}"
        for fila in self._paginado(ruta, {"date": f"{anio_inicio}:{anio_fin}"}):
            iso3 = fila.get("countryiso3code") or (fila.get("country") or {}).get("id")
            if not iso3 or len(iso3) != 3:
                continue
            yield FilaValor(
                codigo_indicador=fila["indicator"]["id"],
                pais_iso3=iso3,
                anio=int(fila["date"]),
                valor=a_float(fila.get("value")),
            )
