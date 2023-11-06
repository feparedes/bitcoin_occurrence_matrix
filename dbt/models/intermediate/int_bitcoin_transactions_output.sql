{{
    config(
        materialized = 'table'
    )
}}


with
bitcoin_transaction as (

    select 
        hash,
        outputs
    from {{ ref("stg_bitcoin_transactions") }}

),

outputs as (

    select * from (
        select
            hash,
            unnest(
                string_to_array(outputs, ',')
            ) as parts
        from
            bitcoin_transaction
    ) as outputs_intermediate
    where
        parts like '%addresses%'
),

cleaned_outputs as (

    select 
        hash,
        split_part(parts,':', 2) as output
    from 
        outputs

),

extracted_outputs as (

    select
        hash,
        split_part(output, '''', 2) as address
    from
        cleaned_outputs

)

select * from extracted_outputs