{{
    config(
        materialized = 'table'
    )
}}


with
addresses as (

    select * from {{ ref("addresses_dict") }}

),

hashes as (

    select * from {{ ref("hash_dict") }}

),

inputs as (

    select * from {{ ref("int_bitcoin_transactions_input") }}

),

codified_inputs as (

    select 
        inputs.hash,
        hashes.equivalent as codified_hash,
        inputs.address,
        addresses.equivalent as codified_address
    from
        inputs
    left join hashes using(hash)
    left join addresses using(address)

)

select * from codified_inputs