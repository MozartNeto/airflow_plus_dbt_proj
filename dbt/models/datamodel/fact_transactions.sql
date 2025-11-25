{{ config(
    materialized='table',
    tags=['fact']
) }}

select
    transaction_id
    ,customer_id
    ,transaction_date
    ,product_id
    ,quantity
    ,price
    ,tax
from {{ ref('cleaned_customer_transactions') }}
