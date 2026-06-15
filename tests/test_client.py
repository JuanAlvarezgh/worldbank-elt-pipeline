import pytest
import responses

from worldbank_extractor.client import WorldBankClient, WorldBankAPIError
from worldbank_extractor.models import ValueRow

IND_URL = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
COUNTRY_URL = "https://api.worldbank.org/v2/country"


@responses.activate
def test_fetch_indicator_paginates_and_maps():
    page1 = [{"page": 1, "pages": 2, "per_page": 1, "total": 2},
             [{"indicator": {"id": "SP.POP.TOTL", "value": "Population"},
               "country": {"id": "US", "value": "United States"},
               "countryiso3code": "USA", "date": "2020", "value": 331}]]
    page2 = [{"page": 2, "pages": 2, "per_page": 1, "total": 2},
             [{"indicator": {"id": "SP.POP.TOTL", "value": "Population"},
               "country": {"id": "CA", "value": "Canada"},
               "countryiso3code": "CAN", "date": "2020", "value": 38}]]
    responses.add(responses.GET, IND_URL, json=page1, status=200)
    responses.add(responses.GET, IND_URL, json=page2, status=200)
    client = WorldBankClient(per_page=1)
    rows = list(client.fetch_indicator("SP.POP.TOTL", 2020, 2020))
    assert rows == [ValueRow("SP.POP.TOTL", "USA", 2020, 331.0),
                    ValueRow("SP.POP.TOTL", "CAN", 2020, 38.0)]


@responses.activate
def test_fetch_indicator_handles_null_value_and_empty_iso():
    payload = [{"page": 1, "pages": 1},
               [{"indicator": {"id": "SP.POP.TOTL"}, "country": {"id": "US"},
                 "countryiso3code": "USA", "date": "2019", "value": None},
                {"indicator": {"id": "SP.POP.TOTL"}, "country": {"id": ""},
                 "countryiso3code": "", "date": "2019", "value": 5}]]
    responses.add(responses.GET, IND_URL, json=payload, status=200)
    rows = list(WorldBankClient().fetch_indicator("SP.POP.TOTL", 2019, 2019))
    assert rows == [ValueRow("SP.POP.TOTL", "USA", 2019, None)]


@responses.activate
def test_get_retries_on_429_then_succeeds():
    responses.add(responses.GET, IND_URL, json={}, status=429)
    responses.add(responses.GET, IND_URL, json=[{"page": 1, "pages": 1}, []], status=200)
    sleeps = []
    client = WorldBankClient(base_delay=0.01, sleep=sleeps.append)
    rows = list(client.fetch_indicator("SP.POP.TOTL", 2020, 2020))
    assert rows == []
    assert len(sleeps) == 1


@responses.activate
def test_get_raises_after_max_retries():
    for _ in range(10):
        responses.add(responses.GET, COUNTRY_URL, json={}, status=503)
    client = WorldBankClient(max_retries=3, base_delay=0.0, sleep=lambda d: None)
    with pytest.raises(WorldBankAPIError):
        list(client.fetch_countries())


@responses.activate
def test_fetch_countries_filters_aggregates():
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
    rows = list(WorldBankClient().fetch_countries())
    assert len(rows) == 1
    assert rows[0].country_iso3 == "USA"
    assert rows[0].capital_city == "Washington D.C."
    assert rows[0].longitude == -77.032


@responses.activate
def test_raises_on_malformed_payload():
    responses.add(responses.GET, COUNTRY_URL, json={"message": "boom"}, status=200)
    with pytest.raises(WorldBankAPIError):
        list(WorldBankClient().fetch_countries())
