{{
    config(
        materialized = 'table'
    )
}}


with
hashes as (

    select 
        distinct 
        hash
    from 
        {{ ref("stg_bitcoin_transactions") }}
    order by 1

),

hashes_dict as (

    select
        hash,
        row_number() over (partition by 1 order by hash) as equivalent
    from
        hashes

)

select * from hashes_dict