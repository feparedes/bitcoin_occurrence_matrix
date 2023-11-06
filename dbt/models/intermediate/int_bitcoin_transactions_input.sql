{{
    config(
        materialized = 'table'
    )
    
}}


with
bitcoin_transaction as (

    select 
        hash,
        inputs
    from {{ ref("stg_bitcoin_transactions") }}

),

inputs as (

    select * from (
        select
            hash,
            unnest(
                string_to_array(inputs, ',')
            ) as parts
        from
            bitcoin_transaction
    ) as input_intermediate
    where
        parts like '%addresses%'
),

cleaned_inputs as (

    select 
        hash,
        split_part(parts,':', 2) as input
    from 
        inputs

),

extracted_inputs as (

    select
        hash,
        split_part(input, '''', 2) as address
    from
        cleaned_inputs

)

select * from extracted_inputs