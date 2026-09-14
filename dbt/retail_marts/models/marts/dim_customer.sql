-- grain: one row per customer (SCD1)
select
    md5(customer_id) as customer_key,
    customer_id,
    full_name,
    segment,
    home_store_id,
    signup_date
from {{ ref('stg_customers') }}
