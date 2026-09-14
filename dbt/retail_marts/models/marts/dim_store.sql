-- grain: one row per store (SCD1)
select
    md5(store_id) as store_key,
    store_id,
    store_name,
    region,
    city,
    store_format,
    opened_date
from {{ ref('stg_stores') }}
