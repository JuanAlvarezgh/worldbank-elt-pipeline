from worldbank_extractor.models import ValueRow, CountryRow, to_float


def test_to_float_handles_none_and_empty():
    assert to_float(None) is None
    assert to_float("") is None
    assert to_float("-77.03") == -77.03
    assert to_float("not-a-number") is None
    assert to_float(42) == 42.0


def test_value_row_is_frozen_dataclass():
    row = ValueRow(indicator_code="SP.POP.TOTL", country_iso3="USA", year=2020, value=1.0)
    assert (row.indicator_code, row.country_iso3, row.year, row.value) == (
        "SP.POP.TOTL", "USA", 2020, 1.0)


def test_country_row_fields():
    row = CountryRow(country_iso3="USA", iso2_code="US", name="United States",
                     region="North America", income_level="High income",
                     capital_city="Washington D.C.", longitude=-77.03, latitude=38.89)
    assert row.country_iso3 == "USA"
    assert row.income_level == "High income"
