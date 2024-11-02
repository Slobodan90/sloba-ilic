with

devices as (

    select * from {{ source('sumup', 'devices') }}

)

select * from devices
