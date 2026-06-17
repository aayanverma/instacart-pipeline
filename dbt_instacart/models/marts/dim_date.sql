{{
    config(
        materialized='table'
    )
}}

-- Grain: one row per (order_dow, order_hour_of_day) combination -- 168 rows total
-- (7 days x 24 hours). This is NOT a conventional calendar date dimension: the
-- Instacart dataset is anonymized to day-of-week + hour-of-day only, with no real
-- calendar dates or timestamps anywhere in the source data. This table is better
-- understood as a "time-of-week bucket" dimension -- worth being explicit about
-- this constraint rather than letting the name "dim_date" imply more than the
-- source data actually supports.
--
-- Built by cross-joining the distinct day-of-week values against a generated
-- sequence of 24 hours, rather than selecting distinct combinations directly out
-- of stg_orders -- this guarantees all 168 combinations exist in the dimension
-- even if the (necessarily incomplete) sample data doesn't happen to contain an
-- order in every single hour of every single day. A dimension table should be
-- complete on its own grain, independent of which combinations the fact table
-- happens to contain.

with day_of_week as (

    select distinct order_dow
    from {{ ref('stg_orders') }}

),

hour_of_day as (

    select seq4() as order_hour_of_day
    from table(generator(rowcount => 24))

),

crossed as (

    select
        d.order_dow,
        h.order_hour_of_day
    from day_of_week d
    cross join hour_of_day h

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_dow', 'order_hour_of_day']) }} as date_key,

        order_dow,
        order_hour_of_day,

        case order_dow
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,

        case when order_dow in (0, 6) then true else false end as is_weekend,

        case
            when order_hour_of_day between 5 and 11 then 'morning'
            when order_hour_of_day between 12 and 16 then 'afternoon'
            when order_hour_of_day between 17 and 20 then 'evening'
            else 'night'
        end as time_of_day_bucket

    from crossed

)

select * from final