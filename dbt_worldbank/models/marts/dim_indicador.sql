select
    codigo_indicador as id_indicador,
    nombre_indicador,
    tema,
    unidad
from {{ ref('indicadores') }}
