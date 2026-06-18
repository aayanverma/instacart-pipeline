{{
    config(
        materialized='table'
    )
}}

-- Grain: one row per (user_id, product_id) pair that the user has ever ordered.
-- This is the JD-driven feature mart addition -- distinct from dim_user (stable,
-- descriptive, one-row-per-user attributes) and from fact_orders (one row per
-- order-product transaction line). This table answers a different question than
-- either: "what's the behavioral signal for this specific user-product pair,"
-- which is the shape a feature/label table needs to take to be useful for an ML
-- workflow (e.g. predicting whether a user will reorder a given product on their
-- next order). One row per (user, product) is the natural grain for that kind of
-- prediction task -- each row IS a candidate (user, product) pair to score.

with fact as (

    select * from {{ ref('fact_orders') }}

),

user_product_aggregates as (

    select
        user_id,
        product_id,

        count(*) as times_purchased,

        -- reorder_rate: fraction of this user's purchases of this product that
        -- were flagged as a reorder, not a first-time purchase. A normalized,
        -- comparable-across-users signal of product affinity -- raw
        -- times_purchased alone conflates "this user orders a lot of things" with
        -- "this user is specifically loyal to this product."
        avg(reordered) as reorder_rate,

        min(order_number) as first_purchased_order_number,
        max(order_number) as last_purchased_order_number,

        -- Proxy for how habitual/top-of-mind this product is for this user --
        -- items added early in a cart plausibly reflect more automatic,
        -- low-consideration purchasing behavior than items added late.
        avg(add_to_cart_order) as avg_add_to_cart_position

    from fact
    group by user_id, product_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['user_id', 'product_id']) }} as user_product_key,

        user_id,
        product_id,
        times_purchased,
        round(reorder_rate, 3) as reorder_rate,
        first_purchased_order_number,
        last_purchased_order_number,
        round(avg_add_to_cart_position, 2) as avg_add_to_cart_position

    from user_product_aggregates

)

select * from final