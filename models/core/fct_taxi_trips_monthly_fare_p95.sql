{{
    config(
        materialized='table'
    )
}}

with fact_trips as (
    select *
    from {{ ref('fact_trips') }}
)

select service_type,
    pickup_year,
    pickup_month,
    percentile_cont(fare_amount, 0.97) over(partition by service_type, pickup_year, pickup_month) as fare_amount_97th_percentile,
    percentile_cont(fare_amount, 0.95) over(partition by service_type, pickup_year, pickup_month) as fare_amount_95th_percentile,
    percentile_cont(fare_amount, 0.90) over(partition by service_type, pickup_year, pickup_month) as fare_amount_90th_percentile
from fact_trips
where fare_amount > 0
    and trip_distance > 0
    and payment_type_description in ('Cash', 'Credit card')
order by service_type,
    pickup_year,
    pickup_month



