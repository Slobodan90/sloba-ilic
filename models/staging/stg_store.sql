with

store as (

    select * from {{ source('sumup', 'store') }}

)

select * from store
