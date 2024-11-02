--5. In order to get AVG time to perform 5 transactions I considered only valid stores which
--had reached at least 5 accepted transactions, on top of that I took day difference between 1st and 5th transaction and then on whole sample I took an avg 
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
           a.device_id,
           a.product_name,
           a.product_sku,
           a.category_name,
           a.amount,
           a.status,
           a.created_at AS transaction_created_at,
           a.happened_at AS transaction_happened_at,
           b.type AS device_type,
           c.id AS store_id,
           c.name AS store_name,
           c.city AS store_city,
           c.country AS store_country,
           c.typology,
           c.customer_id,
           ROW_NUMBER() OVER (PARTITION BY c.name order by a.created_at ASC) AS transaction_rank
    FROM transactions a 
        LEFT JOIN devices b on a.device_id = b.id
        LEFT JOIN store c on b.store_id = c.id
    WHERE status = 'accepted'
)

get_stores_with_at_least_5_transactions AS (
    SELECT store_name,
           COUNT(DISTINCT transaction_id) AS total_transactions
    FROM store_transaction_data
    GROUP BY 1
    HAVING COUNT(DISTINCT transaction_id) >= 5
                                        ),

get_period_per_store AS (
    SELECT store_name,
           MIN(transaction_created_at) AS first_transaction_date,
           MAX(transaction_created_at) AS fifth_transaction_date,
           DATE_DIFF('day', first_transaction_date, fifth_transaction_date) AS days_between_first_and_fifth
    FROM store_transaction_data a
             INNER JOIN get_stores_with_at_least_5_transactions b USING (store_name)
    WHERE a.transaction_rank <= 5
    GROUP BY 1)

SELECT AVG(days_between_first_and_fifth) AS avg_days_till_fifth_transaction
FROM get_period_per_store;
