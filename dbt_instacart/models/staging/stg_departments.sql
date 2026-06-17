with source as (

    select * from {{ source('raw', 'raw_departments') }}

)

select
    department_id,
    department
from source