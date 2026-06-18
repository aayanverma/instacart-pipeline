{{
    config(
        materialized='table'
    )
}}

-- Grain: one row per order-product line -- i.e. one row per product within one
-- order. The unique key at this grain is the COMPOSITE of (order_id, product_id),
-- NOT order_id alone -- a single order legitimately contains many products, so
-- order_id by itself does not uniquely identify a row here. This matters for
-- Layer 4 (incremental materialization): the unique_key config added there must be
-- this composite, or dbt's incremental merge logic would behave incorrectly,
-- silently dropping line items or merging the wrong rows when an already-seen
-- order is reprocessed.
--
-- Built as a full-refresh table for now -- incremental logic is layered on top in
-- Layer 4, deliberately sequenced after this version is verified correct, so
-- correctness is established independent of incremental complexity before that
-- complexity is added.

with order_products as (

    select * from {{ ref('stg_order_products') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

date_dim as (

    select * from {{ ref('dim_date') }}

),

final as (

    select
        -- Surrogate key for the fact row itself, built from the actual grain --
        -- makes this table's primary key explicit and testable (see schema tests,
        -- Layer 5), rather than relying on an implicit composite-column uniqueness
        -- claim that's never directly asserted anywhere.
        {{ dbt_utils.generate_surrogate_key(['op.order_id', 'op.product_id']) }} as order_product_key,

        op.order_id,
        op.product_id,
        o.user_id,
        d.date_key,

        op.add_to_cart_order,
        op.reordered,
        op.source_eval_set,

        o.order_number,
        o.order_dow,
        o.order_hour_of_day,
        o.days_since_prior_order

    from order_products op
    inner join orders o on op.order_id = o.order_id
    left join date_dim d on o.order_dow = d.order_dow and o.order_hour_of_day = d.order_hour_of_day

)

select * from final