with

transactions as (

    select * from {{ ref('stg_transactions') }}

),

devices as (

    select * from {{ ref('stg_devices') }}

),

store_transaction_data AS (
       SELECT a.id AS transaction_id,
              a.device_id,
              a.status,
              b.type AS device_type
       FROM transactions a
                LEFT JOIN devices b on a.device_id = b.id
)

SELECT device_type,
       COUNT(DISTINCT transaction_id) AS transactions_per_type,
       SUM(transactions_per_type) over() AS total_transactions,
       transactions_per_type/total_transactions::FLOAT AS percentage_of_transactions
FROM store_transaction_data
WHERE status = 'accepted'
GROUP BY 1