{{
    config(
        materialized='table'
    )
}}

-- Grain: one row per product_id. Aisle and department names are resolved (joined
-- in) here rather than left as bare foreign keys -- the whole point of a dimension
-- table is to be a self-contained, analysis-ready reference: anyone querying
-- fact_orders and joining to dim_product should get human-readable aisle/department
-- names directly, without needing to know that those live in separate raw tables
-- or write an extra join themselves every time.

with products as (

    select * from {{ ref('stg_products') }}

),

aisles as (

    select * from {{ ref('stg_aisles') }}

),

departments as (

    select * from {{ ref('stg_departments') }}

),

final as (

    select
        p.product_id,
        p.product_name,
        p.aisle_id,
        a.aisle,
        p.department_id,
        d.department
    from products p
    left join aisles a on p.aisle_id = a.aisle_id
    left join departments d on p.department_id = d.department_id

)

select * from final