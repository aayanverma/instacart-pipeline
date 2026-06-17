with source as (

    select * from {{ source('raw', 'raw_products') }}

)

select
    product_id,
    product_name,
    aisle_id,
    department_id
from source