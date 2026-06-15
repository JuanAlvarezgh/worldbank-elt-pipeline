with vals as (
    select * from {{ ref('stg_indicator_values') }}
),
countries as (
    select country_id from {{ ref('dim_country') }}
),
indicators as (
    select indicator_id from {{ ref('dim_indicator') }}
)
select
    -- natural key: country_iso3 + indicator_code + year (aliased below as country_id, indicator_id)
    {{ dbt_utils.generate_surrogate_key(['v.country_iso3', 'v.indicator_code', 'v.year']) }}
        as value_key,
    v.country_iso3   as country_id,
    v.indicator_code as indicator_id,
    v.year,
    v.value
from vals v
inner join countries  c on c.country_id  = v.country_iso3
inner join indicators i on i.indicator_id = v.indicator_code
