with source as (

    select * from {{ source('raw', 'raw_aisles') }}

)

select
    aisle_id,
    aisle
from source