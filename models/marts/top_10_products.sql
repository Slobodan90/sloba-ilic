with

transactions as (

    select * from {{ ref('stg_transactions') }}

)

SELECT product_name,
       COUNT(DISTINCT id) AS total_transactions
FROM transactions
WHERE status = 'accepted'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
