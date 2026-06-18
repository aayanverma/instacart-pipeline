{{
    config(
        materialized='incremental',
        unique_key='order_product_key',
        on_schema_change='fail'
    )
}}

-- Grain: one row per order-product line -- i.e. one row per product within one
-- order. The unique key at this grain is the COMPOSITE of (order_id, product_id),
-- NOT order_id alone -- a single order legitimately contains many products, so
-- order_id by itself does not uniquely identify a row here. Implemented here via
-- the surrogate key `order_product_key` (a hash of order_id + product_id) rather
-- than passing a list of two columns to unique_key -- functionally equivalent for
-- dbt's incremental merge, but a single-column key keeps the merge condition and
-- the eventual schema test (Layer 5) simpler.
--
-- INCREMENTAL STRATEGY:
-- On a first run (target table doesn't exist yet), is_incremental() evaluates to
-- false, the filter below is skipped entirely, and the full historical dataset is
-- loaded -- a complete table build, not a partial one.
-- On every subsequent run, is_incremental() evaluates to true, and the filter below
-- restricts the SELECT to only orders with order_id greater than the max order_id
-- already present in the target table. dbt then MERGEs those new rows in using
-- order_product_key, rather than rebuilding the entire 800K+ row table from scratch.
--
-- Why filter on order_id (a high-water mark), not a timestamp: this dataset has no
-- real order timestamps -- only day-of-week and hour-of-day, which are NOT
-- monotonically increasing over time (Tuesday's order_dow=2 doesn't tell you
-- whether it happened before or after last Friday's order_dow=5). order_id is a
-- reasonable proxy for "newer," but it's worth being explicit that this is an
-- assumption about how the source system assigns IDs (sequential/append-only),
-- not a guarantee -- in a real production source, I'd confirm this with whoever
-- owns the upstream system rather than just assuming it holds.
--
-- on_schema_change='fail': if a future change to this model's column list doesn't
-- match what's already in the target table, fail loudly rather than silently
-- adding/dropping columns or coercing types -- a schema change to a fact table
-- should be a deliberate, reviewed decision, not something that happens implicitly
-- on a routine incremental run.

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

{% if is_incremental() %}
where order_id > (select coalesce(max(order_id), 0) from {{ this }})
{% endif %}