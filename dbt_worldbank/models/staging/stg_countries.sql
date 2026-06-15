with src as (select * from {{ source('raw', 'countries') }})
select
    country_iso3,
    iso2_code,
    name as country_name,
    region,
    income_level,
    capital_city,
    longitude,
    latitude
from src
