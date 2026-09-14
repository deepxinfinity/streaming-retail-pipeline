-- grain: one row per sku x store x day, model-ready
-- every lag/window ends at d-1 so nothing from day d leaks into its own features.
-- lags are calendar-exact (self-joins on d-7/d-14, RANGE window over day numbers for the MA)
-- because gaps in the sales history would otherwise shift a plain LAG().
with agg as (
    select *, datediff(date_day, date'1970-01-01') as day_num
    from {{ ref('agg_sku_store_day') }}
),

with_ma as (
    select
        a.*,
        avg(a.units) over (
            partition by a.sku_id, a.store_id
            order by a.day_num
            range between 28 preceding and 1 preceding
        ) as units_ma_28
    from agg a
),

warmup as (
    select date_add(min(date_day), 28) as min_usable_date from agg
)

select
    f.sku_id,
    f.store_id,
    f.date_day,
    f.units,
    f.eff_price,
    f.list_price,
    round(f.eff_price / p.base_price, 4)  as rel_price,
    f.promo_flag,
    coalesce(f.discount_depth, 0)         as discount_depth,
    l7.units                              as units_lag_7,
    l14.units                             as units_lag_14,
    f.units_ma_28,
    d.dow,
    d.week_of_year,
    d.month,
    cast(d.is_weekend as int)             as is_weekend,
    cast(d.is_holiday as int)             as is_holiday,
    p.category_name,
    p.department,
    p.base_price,
    p.unit_cost,
    s.store_format,
    s.region
from with_ma f
join {{ ref('dim_product') }} p
  on f.sku_id = p.sku_id and p.is_current
join {{ ref('dim_store') }} s on f.store_id = s.store_id
join {{ ref('dim_date') }} d on f.date_day = d.date_day
left join agg l7
  on f.sku_id = l7.sku_id and f.store_id = l7.store_id
 and l7.date_day = date_sub(f.date_day, 7)
left join agg l14
  on f.sku_id = l14.sku_id and f.store_id = l14.store_id
 and l14.date_day = date_sub(f.date_day, 14)
cross join warmup w
where f.date_day >= w.min_usable_date
