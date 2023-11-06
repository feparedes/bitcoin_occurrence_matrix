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
    from {{ source("public", "bitcoin_raw_data") }}

)

select * from bitcoin_transaction