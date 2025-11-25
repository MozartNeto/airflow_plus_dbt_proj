{{ config(
    materialized='table',
    tags=['dimension']
) }}

select
    product_id as id
    ,product_name
from {{ ref('cleaned_customer_transactions') }}
