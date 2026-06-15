from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FilaValor:
    codigo_indicador: str
    pais_iso3: str
    anio: int
    valor: float | None


@dataclass(frozen=True)
class FilaPais:
    pais_iso3: str
    codigo_iso2: str | None
    nombre: str | None
    region: str | None
    nivel_ingreso: str | None
    ciudad_capital: str | None
    longitud: float | None
    latitud: float | None


def a_float(valor) -> float | None:
    if valor is None or valor == "":
        return None
    try:
        return float(valor)
    except (TypeError, ValueError):
        return None
