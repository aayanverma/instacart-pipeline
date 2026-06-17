with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

renamed_and_cast as (

    select
        order_id,
        user_id,
        eval_set,
        order_number,
        order_dow,

        -- Explicit cast, not left to inference. The raw column is stored as a
        -- zero-padded string ("08") to preserve exact source fidelity in the raw
        -- layer (see sources.yml). Casting to integer here is a deliberate staging
        -- decision: order_hour_of_day is fundamentally a numeric quantity (you can
        -- compare/bucket/aggregate on it), and the zero-padding was a string
        -- formatting artifact of the source CSV, not meaningful information in
        -- itself -- "08" and "8" represent the same hour.
        cast(order_hour_of_day as integer) as order_hour_of_day,

        -- Deliberately NOT coalesced to 0 or any other sentinel value. NULL here
        -- means "this user's first order, no prior order exists to measure
        -- against" -- a different thing from "this value is missing/unknown."
        -- Coalescing to 0 would make first orders look like they happened zero
        -- days after a prior order, which is factually wrong, not just a stylistic
        -- choice. See sources.yml and BUILD_LOG.md for the same reasoning applied
        -- to the schema test on this column (no blanket not_null test).
        days_since_prior_order

    from source

)

select * from renamed_and_cast