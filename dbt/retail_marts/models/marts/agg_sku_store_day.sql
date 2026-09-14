-- grain: one row per sku x store x day (days with at least one sale)
with sales as (
    select
        sku_id,
        store_id,
        event_date                       as date_day,
        sum(qty)                         as units,
        sum(line_amount)                 as revenue,
        sum(margin_amount)               as margin,
        max(case when on_promo then 1 else 0 end) as promo_flag
    from {{ ref('fct_sales') }}
    group by 1, 2, 3
)

select
    s.sku_id,
    s.store_id,
    s.date_day,
    s.units,
    s.revenue,
    s.margin,
    round(s.revenue / s.units, 4)                    as eff_price,
    ph.price                                         as list_price,
    s.promo_flag,
    round(1 - (s.revenue / s.units) / ph.price, 4)   as discount_depth
from sales s
left join {{ ref('stg_price_history') }} ph
  on s.sku_id = ph.sku_id
 and ph.scope = 'all'
 and s.date_day between ph.valid_from and ph.valid_to
