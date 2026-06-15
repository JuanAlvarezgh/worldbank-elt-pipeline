-- Consultas analiticas de ejemplo sobre el esquema en estrella del Banco Mundial (marts.*)
-- Ejecutar con: docker compose exec warehouse-postgres psql -U warehouse -f /dev/stdin < docs/consultas_ejemplo.sql
-- o pegar cada una en psql.

-- 1) Top 10 paises por PIB per capita en 2020
select c.nombre_pais, f.valor as pib_per_capita_usd
from marts.hechos_valor_indicador f
join marts.dim_pais      c on c.id_pais      = f.id_pais
join marts.dim_indicador i on i.id_indicador = f.id_indicador
where i.id_indicador = 'NY.GDP.PCAP.CD'
  and f.anio = 2020
order by f.valor desc nulls last
limit 10;

-- 2) Tendencia de esperanza de vida para Colombia
select f.anio, f.valor as esperanza_vida_anios
from marts.hechos_valor_indicador f
join marts.dim_indicador i on i.id_indicador = f.id_indicador
where f.id_pais = 'COL'
  and i.id_indicador = 'SP.DYN.LE00.IN'
order by f.anio;

-- 3) Poblacion promedio por region en el ultimo anio disponible por indicador
select c.region, round(avg(f.valor)) as poblacion_promedio
from marts.hechos_valor_indicador f
join marts.dim_pais c on c.id_pais = f.id_pais
where f.id_indicador = 'SP.POP.TOTL'
  and f.anio = 2023
  and c.region is not null
group by c.region
order by poblacion_promedio desc;
