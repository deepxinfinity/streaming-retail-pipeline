-- light renames/casts only; business logic lives in marts
select
    sale_line_id,
    channel,
    txn_id,
    store_id,
    customer_id,
    sku_id,
    qty,
    unit_price,
    line_amount,
    promo_id,
    event_ts,
    cast(event_ts as date) as event_date,
    arrived_ts
from {{ source('silver', 'sales_lines') }}
