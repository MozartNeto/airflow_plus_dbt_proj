{{ config(
    materialized='table',
    tags=['staging']
) }}

select
    transaction_id
    ,customer_id
    ,CASE
        WHEN transaction_date LIKE '____-__-__' THEN TO_DATE(transaction_date, 'YYYY-MM-DD')
        WHEN transaction_date LIKE '__-__-____' THEN TO_DATE(transaction_date, 'DD-MM-YYYY')
        WHEN transaction_date LIKE '__ ___ ____' THEN TO_DATE(transaction_date, 'DD Mon YYYY')
        WHEN transaction_date LIKE '__/__/____' THEN TO_DATE(transaction_date, 'MM/DD/YYYY')
        ELSE NULL 
    END AS transaction_date
    ,product_id
    ,product_name
    ,quantity
    ,price
    ,tax
from {{ source('raw_data', 'raw_customer_transactions') }}
where 1=1
    and (tax is not null and tax ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    and (price is not null and price ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    and (transaction_id is not null and transaction_id ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    and (product_id is not null and product_id ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    and (customer_id is not null and customer_id ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    and (quantity is not null and quantity ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
    




