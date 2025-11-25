{{ config(
    materialized='table',
    tags=['dimension']
) }}

select
    transaction_date as date_key,
    EXTRACT(YEAR FROM transaction_date)::int as year,
    EXTRACT(MONTH FROM transaction_date)::int as month,
    EXTRACT(DAY FROM transaction_date)::int as day,
    EXTRACT(DOW FROM transaction_date)::int as day_of_week,
    EXTRACT(QUARTER FROM transaction_date)::int as quarter
from {{ ref('cleaned_customer_transactions') }}

