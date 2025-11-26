{{ config(
    materialized='table',
    tags=['staging']
) }}

SELECT
    CAST(transaction_id AS NUMERIC) AS transaction_id,
    CAST(customer_id AS NUMERIC) AS customer_id,
    CASE
        WHEN transaction_date LIKE '____-__-__' THEN TO_DATE(transaction_date, 'YYYY-MM-DD')
        WHEN transaction_date LIKE '__-__-____' THEN TO_DATE(transaction_date, 'DD-MM-YYYY')
        WHEN transaction_date LIKE '__ ___ ____' THEN TO_DATE(transaction_date, 'DD Mon YYYY')
        WHEN transaction_date LIKE '__/__/____' THEN TO_DATE(transaction_date, 'MM/DD/YYYY')
        ELSE NULL 
    END AS transaction_date,
    CAST(product_id AS NUMERIC) AS product_id,
    CAST(product_name AS VARCHAR(255)) AS product_name,
    CAST(quantity AS NUMERIC) AS quantity,
    CAST(price AS NUMERIC(10,2)) AS price,
    CAST(tax AS NUMERIC(10,2)) AS tax
FROM {{ source('raw_data', 'raw_customer_transactions') }}
WHERE 1=1
    AND (tax IS NOT NULL AND tax ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    AND (price IS NOT NULL AND price ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    AND (transaction_id IS NOT NULL AND transaction_id ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    AND (product_id IS NOT NULL AND product_id ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    AND (customer_id IS NOT NULL AND customer_id ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    AND (quantity IS NOT NULL AND quantity ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
