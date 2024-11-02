--I have used this model to build tableau report
with

transactions as (

    select * from {{ ref('stg_transactions') }}

),

store as (

    select * from {{ ref('stg_store') }}

),

devices as (

    select * from {{ ref('stg_devices') }}

)

SELECT a.id AS transaction_id,
       a.device_id,
       a.product_name,
       a.product_sku,
       a.category_name,
       a.amount,
       a.status,
       a.created_at AS transaction_created_at,
       b.type AS device_type,
       c.id AS store_id,
       c.name AS store_name,
       c.city AS store_city,
       c.country AS store_country,
       c.typology,
       c.customer_id
FROM transactions a
         LEFT JOIN devices b on a.device_id = b.id
         LEFT JOIN store c on b.store_id = c.id
