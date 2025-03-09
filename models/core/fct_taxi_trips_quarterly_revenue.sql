{{
    config(
        materialized='table'
    )
}}

with fact_trips as (
    select *
    from {{ ref('fact_trips') }}
),
quarterly_revenue as (
    select service_type,
        pickup_year,
        pickup_quarter,
        sum(total_amount) as quarterly_total
    from fact_trips
    where pickup_year between 2019 and 2020
    group by service_type,
        pickup_year,
        pickup_quarter
    order by service_type,
        pickup_year,
        pickup_quarter
)

select *,
    case 
        when prev_revenue is not null
        then ((revenue - prev_revenue)/prev_revenue)*100
    end as yoy_growth
from (
    select curr.service_type,
        curr.pickup_year,
        curr.pickup_quarter,
        curr.quarterly_total as revenue,
        prev.quarterly_total as prev_revenue,
    from quarterly_revenue curr
    left join quarterly_revenue prev 
        on curr.pickup_quarter = prev.pickup_quarter
        and curr.pickup_year = prev.pickup_year + 1
        and curr.service_type = prev.service_type
)
order by service_type,
        pickup_year,
        pickup_quarter