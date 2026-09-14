select
    customer_id,
    full_name,
    segment,
    home_store_id,
    signup_date
from {{ source('silver', 'dim_customers') }}
