select
    country_iso3 as country_id,
    country_name,
    region,
    income_level,
    capital_city,
    longitude,
    latitude
from {{ ref('stg_countries') }}
