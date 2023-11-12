{{
    config(
        materialized = 'table'
    )
}}


with
bitcoin_transaction as (

    select 
        hash,
        inputs,
        outputs
    from 
        {{ source("public", "bitcoin_raw_data") }}
    where
        replace(left(block_timestamp, 7), '-', '') between '{{ var("initial_date") }}' and '{{ var("end_date") }}'

)

select * from bitcoin_transaction