{{ config(
    materialized='table',
    tags=['quarantine']
) }}

with exceptions_tax_field as (
    select *, case when tax = null then 'The tax field is NULL' else 'The tax field does not match numeric datatype' end as error_message
    from {{ source('raw_data', 'raw_customer_transactions') }}
    where tax is null or not (tax ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
),
exceptions_price_field as (
    select *, case when price = null then 'The price field is NULL' else 'The price field does not match numeric datatype' end as error_message
    from {{ source('raw_data', 'raw_customer_transactions') }}
    where price is null or not (price ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
),
exceptions_transaction_id_field as (
    select *, case when transaction_id = null then 'The transaction_id field is NULL' else 'The transaction_id field does not match numeric datatype' end as error_message
    from {{ source('raw_data', 'raw_customer_transactions') }}
    where transaction_id is null or not (transaction_id ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
),
exceptions_product_id_field as (
    select *, case when product_id = null then 'The product_id field is NULL' else 'The product_id field does not match numeric datatype' end as error_message
    from {{ source('raw_data', 'raw_customer_transactions') }}
    where product_id is null or not (product_id ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
),
exceptions_customer_id_field as (
    select *, case when customer_id = null then 'The customer_id field is NULL' else 'The customer_id field does not match numeric datatype' end as error_message
    from {{ source('raw_data', 'raw_customer_transactions') }}
    where customer_id is null or not (customer_id ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
),
exceptions_quantity_field as (
    select *, case when quantity = null then 'The quantity field is NULL' else 'The quantity field does not match numeric datatype' end as error_message
    from {{ source('raw_data', 'raw_customer_transactions') }}
    where quantity is null or not (quantity ~ '^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
)
select *
from exceptions_tax_field
union all 
    (select * from exceptions_price_field)
union all 
    (select * from exceptions_customer_id_field)
union all 
    (select * from exceptions_product_id_field)
union all 
    (select * from exceptions_quantity_field)
union all 
    (select * from exceptions_transaction_id_field)
