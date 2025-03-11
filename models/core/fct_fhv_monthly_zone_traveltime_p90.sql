{{
    config(
        materialized='table'
    )
}}

with fhv_trips as (
    select *
    from {{ ref('dim_fhv_trips') }}
),
trip_duration as (
    select 
)

select pickup_year,
    pickup_month,
    pickup_locationid,
    dropoff_locationid,

    percentile_cont(fare_amount, 0.90) over(partition by service_type, pickup_year, pickup_month) as fare_amount_90th_percentile
from fhv_trips

order by service_type,
    pickup_year,
    pickup_month



