-- custom test: price validity windows must never overlap per sku + scope
select
    a.sku_id, a.scope,
    a.valid_from as a_from, a.valid_to as a_to,
    b.valid_from as b_from, b.valid_to as b_to
from {{ ref('stg_price_history') }} a
join {{ ref('stg_price_history') }} b
  on a.sku_id = b.sku_id
 and a.scope = b.scope
 and a.valid_from < b.valid_from          -- each unordered pair once
 and a.valid_to >= b.valid_from           -- overlap
