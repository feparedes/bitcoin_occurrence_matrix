{{
    config(
        materialized = 'table'
    )
}}


with
inputs as (

    select * from {{ ref("bitcoin_transactions_input") }}

),

inputs_with_number_of_addresses as (

    select 
        codified_hash,
        row_number() over (partition by codified_hash) as address_codified_based_on_hash
    from
        inputs

),

inputs_grouped as (

    select
        codified_hash,
        least(max(address_codified_based_on_hash), 20) as number_of_addresses
    from
        inputs_with_number_of_addresses
    group by 
        1

),

inputs_with_sets as (

    select
		row_number() over () as set_addresses,
        codified_hash,
        number_of_addresses
    from 
        inputs_grouped
        
)

select * from inputs_with_sets