select
    indicator_code as indicator_id,
    indicator_name,
    topic,
    unit
from {{ ref('indicators') }}
