-- grain: one row per sale LINE (sale_line_id unique)
-- the as-of join is the fiddly bit: margin uses the unit_cost that was valid at sale time,
-- not today's cost
select
    s.sale_line_id,
    p.product_key,
    st.store_key,
    c.customer_key,
    d.date_key,
    s.event_date,
    s.channel,
    s.txn_id,
    s.sku_id,
    s.store_id,
    s.qty,
    s.unit_price,
    s.line_amount,
    cast((s.unit_price - p.unit_cost) * s.qty as decimal(11,2)) as margin_amount,
    s.promo_id,
    s.promo_id is not null                                      as on_promo,
    s.event_ts,
    s.arrived_ts
from {{ ref('stg_sales') }} s
join {{ ref('dim_product') }} p
  on s.sku_id = p.sku_id
 and s.event_date between p.valid_from and p.valid_to
join {{ ref('dim_store') }} st on s.store_id = st.store_id
left join {{ ref('dim_customer') }} c on s.customer_id = c.customer_id
join {{ ref('dim_date') }} d on s.event_date = d.date_day
