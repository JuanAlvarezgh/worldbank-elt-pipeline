with vals as (
    select * from {{ ref('stg_valores_indicador') }}
),
paises as (
    select id_pais from {{ ref('dim_pais') }}
),
indicadores as (
    select id_indicador from {{ ref('dim_indicador') }}
)
select
    -- clave natural: pais_iso3 + codigo_indicador + anio (aliasados abajo como id_pais, id_indicador)
    {{ dbt_utils.generate_surrogate_key(['v.pais_iso3', 'v.codigo_indicador', 'v.anio']) }}
        as llave_valor,
    v.pais_iso3         as id_pais,
    v.codigo_indicador  as id_indicador,
    v.anio,
    v.valor
from vals v
inner join paises     p on p.id_pais      = v.pais_iso3
inner join indicadores i on i.id_indicador = v.codigo_indicador
