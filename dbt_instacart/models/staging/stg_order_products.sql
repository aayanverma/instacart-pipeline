with prior as (

    select * from {{ source('raw', 'raw_order_products_prior') }}

),

train as (

    select * from {{ source('raw', 'raw_order_products_train') }}

),

-- raw_order_products_prior and raw_order_products_train are schema-identical --
-- the split exists only because of the dataset's original Kaggle ML-competition
-- framing (prior orders = historical context, train = the competition's labeled
-- set), which has nothing to do with this project's needs. Unioning them here,
-- explicitly, in staging -- rather than silently combining them during ingestion --
-- keeps the raw layer a faithful mirror of the source files and makes this
-- reconciliation decision visible and testable rather than implicit.
unioned as (

    select
        order_id,
        product_id,
        add_to_cart_order,
        reordered,
        'prior' as source_eval_set
    from prior

    union all

    select
        order_id,
        product_id,
        add_to_cart_order,
        reordered,
        'train' as source_eval_set
    from train

)

select * from unioned