from worldbank_extractor.models import FilaValor, FilaPais, a_float


def test_a_float_maneja_none_y_vacio():
    assert a_float(None) is None
    assert a_float("") is None
    assert a_float("-77.03") == -77.03
    assert a_float("no-es-numero") is None
    assert a_float(42) == 42.0


def test_fila_valor_es_dataclass_frozen():
    fila = FilaValor(codigo_indicador="SP.POP.TOTL", pais_iso3="USA", anio=2020, valor=1.0)
    assert (fila.codigo_indicador, fila.pais_iso3, fila.anio, fila.valor) == (
        "SP.POP.TOTL", "USA", 2020, 1.0)


def test_fila_pais_campos():
    fila = FilaPais(pais_iso3="USA", codigo_iso2="US", nombre="United States",
                    region="North America", nivel_ingreso="High income",
                    ciudad_capital="Washington D.C.", longitud=-77.03, latitud=38.89)
    assert fila.pais_iso3 == "USA"
    assert fila.nivel_ingreso == "High income"
