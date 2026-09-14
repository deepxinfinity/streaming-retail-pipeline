select
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
    coalesce(valid_to, date'9999-12-31') as valid_to,
    is_current
from {{ source('silver', 'dim_products') }}
