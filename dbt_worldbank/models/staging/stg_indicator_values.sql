with src as (select * from {{ source('raw', 'indicator_values') }})
select
    indicator_code,
    country_iso3,
    year,
    value
from src
where value is not null
