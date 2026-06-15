select
    pais_iso3       as id_pais,
    codigo_iso2,
    nombre_pais,
    region,
    nivel_ingreso,
    ciudad_capital,
    longitud,
    latitud
from {{ ref('stg_paises') }}
