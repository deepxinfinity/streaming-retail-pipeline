-- grain: one row per calendar day, 2024-01-01 -> today + 1 year
with spine as (
    select explode(sequence(date'2024-01-01', add_months(current_date(), 12), interval 1 day)) as date_day
)

select
    s.date_day,
    date_format(s.date_day, 'yyyyMMdd')       as date_key,
    dayofweek(s.date_day)                     as dow,          -- 1=Sunday ... 7=Saturday
    weekofyear(s.date_day)                    as week_of_year,
    month(s.date_day)                         as month,
    quarter(s.date_day)                       as quarter,
    year(s.date_day)                          as year,
    dayofweek(s.date_day) in (1, 7)           as is_weekend,
    h.holiday_date is not null                as is_holiday,
    h.holiday_name
from spine s
left join {{ ref('holidays') }} h on s.date_day = h.holiday_date
