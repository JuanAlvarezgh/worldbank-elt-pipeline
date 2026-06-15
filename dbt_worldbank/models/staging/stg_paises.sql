with src as (select * from {{ source('raw', 'paises') }})
select
    pais_iso3,
    codigo_iso2,
    nombre as nombre_pais,
    region,
    nivel_ingreso,
    ciudad_capital,
    longitud,
    latitud
from src
