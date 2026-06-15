import pytest
import requests
import responses

from worldbank_extractor.client import ClienteBancoMundial, ErrorAPIBancoMundial
from worldbank_extractor.models import FilaValor

IND_URL = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
COUNTRY_URL = "https://api.worldbank.org/v2/country"


@responses.activate
def test_obtener_indicador_pagina_y_mapea():
    pagina1 = [{"page": 1, "pages": 2, "per_page": 1, "total": 2},
               [{"indicator": {"id": "SP.POP.TOTL", "value": "Population"},
                 "country": {"id": "US", "value": "United States"},
                 "countryiso3code": "USA", "date": "2020", "value": 331}]]
    pagina2 = [{"page": 2, "pages": 2, "per_page": 1, "total": 2},
               [{"indicator": {"id": "SP.POP.TOTL", "value": "Population"},
                 "country": {"id": "CA", "value": "Canada"},
                 "countryiso3code": "CAN", "date": "2020", "value": 38}]]
    responses.add(responses.GET, IND_URL, json=pagina1, status=200)
    responses.add(responses.GET, IND_URL, json=pagina2, status=200)
    cliente = ClienteBancoMundial(por_pagina=1)
    filas = list(cliente.obtener_indicador("SP.POP.TOTL", 2020, 2020))
    assert filas == [FilaValor("SP.POP.TOTL", "USA", 2020, 331.0),
                     FilaValor("SP.POP.TOTL", "CAN", 2020, 38.0)]


@responses.activate
def test_obtener_indicador_maneja_valor_null_e_iso_vacio():
    payload = [{"page": 1, "pages": 1},
               [{"indicator": {"id": "SP.POP.TOTL"}, "country": {"id": "US"},
                 "countryiso3code": "USA", "date": "2019", "value": None},
                {"indicator": {"id": "SP.POP.TOTL"}, "country": {"id": ""},
                 "countryiso3code": "", "date": "2019", "value": 5}]]
    responses.add(responses.GET, IND_URL, json=payload, status=200)
    filas = list(ClienteBancoMundial().obtener_indicador("SP.POP.TOTL", 2019, 2019))
    assert filas == [FilaValor("SP.POP.TOTL", "USA", 2019, None)]


@responses.activate
def test_obtener_reintenta_en_429_luego_ok():
    responses.add(responses.GET, IND_URL, json={}, status=429)
    responses.add(responses.GET, IND_URL, json=[{"page": 1, "pages": 1}, []], status=200)
    esperas = []
    cliente = ClienteBancoMundial(retardo_base=0.01, dormir=esperas.append)
    filas = list(cliente.obtener_indicador("SP.POP.TOTL", 2020, 2020))
    assert filas == []
    assert esperas == [0.01]  # backoff = retardo_base * 2**0


@responses.activate
def test_obtener_lanza_tras_max_reintentos():
    for _ in range(10):
        responses.add(responses.GET, COUNTRY_URL, json={}, status=503)
    cliente = ClienteBancoMundial(max_reintentos=3, retardo_base=0.0, dormir=lambda d: None)
    with pytest.raises(ErrorAPIBancoMundial):
        list(cliente.obtener_paises())


@responses.activate
def test_obtener_reintenta_en_timeout_de_red_luego_ok():
    responses.add(responses.GET, IND_URL, body=requests.exceptions.ReadTimeout("boom"))
    responses.add(responses.GET, IND_URL, json=[{"page": 1, "pages": 1}, []], status=200)
    esperas = []
    cliente = ClienteBancoMundial(retardo_base=0.01, dormir=esperas.append)
    filas = list(cliente.obtener_indicador("SP.POP.TOTL", 2020, 2020))
    assert filas == []
    assert esperas == [0.01]


@responses.activate
def test_obtener_lanza_tras_max_reintentos_en_timeout():
    for _ in range(10):
        responses.add(responses.GET, COUNTRY_URL, body=requests.exceptions.ConnectionError("caido"))
    cliente = ClienteBancoMundial(max_reintentos=2, retardo_base=0.0, dormir=lambda d: None)
    with pytest.raises(ErrorAPIBancoMundial):
        list(cliente.obtener_paises())


@responses.activate
def test_obtener_paises_filtra_agregados():
    payload = [{"page": 1, "pages": 1},
               [{"id": "USA", "iso2Code": "US", "name": "United States",
                 "region": {"id": "NAC", "value": "North America"},
                 "incomeLevel": {"value": "High income"}, "capitalCity": "Washington D.C.",
                 "longitude": "-77.032", "latitude": "38.889"},
                {"id": "WLD", "iso2Code": "1W", "name": "World",
                 "region": {"id": "NA", "value": "Aggregates"},
                 "incomeLevel": {"value": "Aggregates"}, "capitalCity": "",
                 "longitude": "", "latitude": ""}]]
    responses.add(responses.GET, COUNTRY_URL, json=payload, status=200)
    filas = list(ClienteBancoMundial().obtener_paises())
    assert len(filas) == 1
    assert filas[0].pais_iso3 == "USA"
    assert filas[0].ciudad_capital == "Washington D.C."
    assert filas[0].longitud == -77.032


@responses.activate
def test_lanza_con_payload_malformado():
    responses.add(responses.GET, COUNTRY_URL, json={"message": "boom"}, status=200)
    with pytest.raises(ErrorAPIBancoMundial):
        list(ClienteBancoMundial().obtener_paises())


@responses.activate
def test_lanza_con_metadata_pages_malformada():
    payload = [{"page": 1, "pages": "muchas"},
               [{"indicator": {"id": "SP.POP.TOTL"}, "countryiso3code": "USA",
                 "date": "2020", "value": 1}]]
    responses.add(responses.GET, IND_URL, json=payload, status=200)
    with pytest.raises(ErrorAPIBancoMundial):
        list(ClienteBancoMundial().obtener_indicador("SP.POP.TOTL", 2020, 2020))


@responses.activate
def test_obtener_indicador_envia_rango_de_anios():
    responses.add(responses.GET, IND_URL, json=[{"page": 1, "pages": 1}, []], status=200)
    list(ClienteBancoMundial().obtener_indicador("SP.POP.TOTL", 2018, 2022))
    assert "date=2018%3A2022" in responses.calls[0].request.url


def test_cliente_context_manager_cierra_sesion():
    class SesionFalsa:
        def __init__(self):
            self.cerrada = False

        def close(self):
            self.cerrada = True

    sesion = SesionFalsa()
    with ClienteBancoMundial(sesion=sesion) as cliente:
        assert cliente.sesion is sesion
    assert sesion.cerrada is True
