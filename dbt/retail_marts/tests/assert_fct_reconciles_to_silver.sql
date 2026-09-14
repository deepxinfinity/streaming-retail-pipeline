-- custom test: gold must reconcile to silver EXACTLY  -  the quality gate between layers.
-- fails (returns a row) if row counts or revenue sums drift.
with f as (
    select count(*) as n, coalesce(sum(line_amount), 0) as amt from {{ ref('fct_sales') }}
),
s as (
    select count(*) as n, coalesce(sum(line_amount), 0) as amt
    from {{ source('silver', 'sales_lines') }}
)

select f.n as fct_rows, s.n as silver_rows, f.amt as fct_amount, s.amt as silver_amount
from f cross join s
where f.n <> s.n or abs(f.amt - s.amt) > 0.01
