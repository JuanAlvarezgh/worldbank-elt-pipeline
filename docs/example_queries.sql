-- Example analytical queries against the World Bank star schema (marts.*)
-- Run with: docker compose exec warehouse-postgres psql -U warehouse -f /dev/stdin < docs/example_queries.sql
-- or paste individually into psql.

-- 1) Top 10 countries by GDP per capita in 2020
select c.country_name, f.value as gdp_per_capita_usd
from marts.fact_indicator_value f
join marts.dim_country   c on c.country_id   = f.country_id
join marts.dim_indicator i on i.indicator_id = f.indicator_id
where i.indicator_id = 'NY.GDP.PCAP.CD'
  and f.year = 2020
order by f.value desc nulls last
limit 10;

-- 2) Life expectancy trend for Colombia
select f.year, f.value as life_expectancy_years
from marts.fact_indicator_value f
join marts.dim_indicator i on i.indicator_id = f.indicator_id
where f.country_id = 'COL'
  and i.indicator_id = 'SP.DYN.LE00.IN'
order by f.year;

-- 3) Average population by region in the latest available year per indicator
select c.region, round(avg(f.value)) as avg_population
from marts.fact_indicator_value f
join marts.dim_country c on c.country_id = f.country_id
where f.indicator_id = 'SP.POP.TOTL'
  and f.year = 2023
  and c.region is not null
group by c.region
order by avg_population desc;
