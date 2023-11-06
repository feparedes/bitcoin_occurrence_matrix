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

outputs as (

    select * from {{ ref("int_bitcoin_transactions_output") }}

),

codified_outputs as (

    select 
        outputs.hash,
        hashes.equivalent as codified_hash,
        outputs.address,
        addresses.equivalent as codified_address
    from
        outputs
    left join hashes using(hash)
    left join addresses using(address)

)

select * from codified_outputs