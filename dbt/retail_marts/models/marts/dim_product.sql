-- grain: one row per sku version (scd2).  join facts on sku_id + event date within [valid_from, valid_to]
select
    md5(concat(sku_id, '|', cast(valid_from as string))) as product_key,
    sku_id,
    sku_name,
    department,
    category_id,
    category_name,
    brand,
    base_price,
    unit_cost,
    active,
    valid_from,
    valid_to,
    is_current
from {{ ref('stg_products') }}
