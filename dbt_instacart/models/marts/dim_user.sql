{{
    config(
        materialized='table'
    )
}}

-- Grain: one row per user_id. Holds stable, descriptive attributes about a user's
-- overall order history -- total orders placed, first/most recent order timing,
-- account tenure in terms of order count. Deliberately does NOT include
-- product-level behavioral signals (reorder rate on a specific product, days since
-- last purchase of a specific product) -- those live in fct_user_product_features
-- instead, at user-product grain, since they answer a different question ("what's
-- the predictive signal for this user-product pair") for a different audience
-- (ML feature consumption vs. general BI/reporting) than this table does.

with orders as (

    select * from {{ ref('stg_orders') }}

),

user_aggregates as (

    select
        user_id,
        count(distinct order_id) as total_orders,
        min(order_number) as first_order_number,
        max(order_number) as most_recent_order_number,

        -- avg_days_between_orders intentionally excludes NULL days_since_prior_order
        -- (i.e. excludes each user's first order from the average) -- including a
        -- NULL-as-zero would understate every user's true ordering cadence, since a
        -- first order isn't "zero days after a prior order," it's "no prior order
        -- exists." AVG() in Snowflake already ignores NULLs by default, but this is
        -- called out explicitly here rather than left implicit, since it's exactly
        -- the kind of silent correctness assumption worth being able to explain.
        avg(days_since_prior_order) as avg_days_between_orders,

        max(order_dow) as most_recent_order_dow,
        max(order_hour_of_day) as most_recent_order_hour

    from orders
    group by user_id

),

final as (

    select
        user_id,
        total_orders,
        first_order_number,
        most_recent_order_number,
        round(avg_days_between_orders, 1) as avg_days_between_orders,

        -- Simple tenure segmentation -- a common, useful pattern for a user
        -- dimension: lets downstream reporting/BI slice by customer maturity
        -- without recomputing this bucketing logic in every query.
        case
            when total_orders = 1 then 'one_time'
            when total_orders between 2 and 5 then 'occasional'
            when total_orders between 6 and 15 then 'regular'
            else 'frequent'
        end as customer_segment

    from user_aggregates

)

select * from final