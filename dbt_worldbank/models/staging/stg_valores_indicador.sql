with src as (select * from {{ source('raw', 'valores_indicador') }})
select
    codigo_indicador,
    pais_iso3,
    anio,
    valor
from src
where valor is not null
