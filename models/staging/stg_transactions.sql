with

transactions as (

    select * from {{ source('sumup', 'transactions') }}

)

select * from transactions
