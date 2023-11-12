{{
    config(
        materialized = 'table'
    )
}}


with
input_addresses as (

    select 
        distinct 
        address
    from 
        {{ ref("int_bitcoin_transactions_input") }}

),

output_addresses as (

    select 
        distinct 
        address
    from 
        {{ ref("int_bitcoin_transactions_output") }}

),

union_addresses as (

    select * from input_addresses

    union all

    select * from output_addresses
),

addresses as (

    select
        distinct
        address
    from
        union_addresses

),

addresses_dict as (

    select
        address,
        row_number() over (partition by 1 order by address) as equivalent
    from
        addresses

)

select * from addresses_dict