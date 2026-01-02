
{{ config(materialized='table') }}

WITH all_payments AS (
    SELECT 
        processor_name,
        CAST(payment_creation_date AS DATE) AS payment_date
    FROM {{ ref('fact_payments') }}
    WHERE payment_status IN ('AUTHORIZED', 'CANCELLED', 'SETTLING', 'SETTLED', 'PARTIALLY_SETTLED')
),

payments_per_day AS (
    SELECT
        processor_name,
        payment_date,
        COUNT(*) AS authorized_payments
    FROM all_payments
    GROUP BY processor_name, payment_date
)

SELECT
    processor_name,
    ROUND(AVG(authorized_payments), 1) AS mean_authorized_payments_per_day
FROM payments_per_day
GROUP BY processor_name 
ORDER BY mean_authorized_payments_per_day DESC;
