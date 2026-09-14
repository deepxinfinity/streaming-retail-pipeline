select
    store_id,
    store_name,
    region,
    city,
    store_format,
    opened_date
from {{ source('silver', 'dim_stores') }}
