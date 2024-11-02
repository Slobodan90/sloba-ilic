with

transactions as (

    select * from {{ ref('stg_transactions') }}

),

store as (

    select * from {{ ref('stg_store') }}

),

devices as (

    select * from {{ ref('stg_devices') }}

),

store_transaction_data AS (
       SELECT a.id AS transaction_id,
              a.amount,
              a.status,
              c.country AS store_country,
              c.typology
       FROM transactions a
                LEFT JOIN devices b on a.device_id = b.id
                LEFT JOIN store c on b.store_id = c.id
)

SELECT store_country,
       typology,
       COUNT(DISTINCT transaction_id) AS total_transactions,
       SUM(amount) AS total_amount,
       total_amount/total_transactions::FLOAT AS avg_amount
FROM store_transaction_data
WHERE status = 'accepted'
GROUP BY 1,2;
