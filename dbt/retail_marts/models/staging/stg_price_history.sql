select
    sku_id,
    scope,
    price,
    valid_from,
    coalesce(valid_to, date'9999-12-31') as valid_to
from {{ source('silver', 'price_history') }}
