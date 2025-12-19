
{{ config(materialized='incremental', unique_key='payment_id') }}

WITH staged_payments AS (
    SELECT * FROM {{ ref('stg_payment') }}
),

linked_customers AS (
    SELECT
        COALESCE(
         json_extract_string(p.cleaned_customer_details, '$.customer_id')::VARCHAR, 
         json_extract_string(p.cleaned_customer_details, '$.email_address')::VARCHAR,
         'UNKNOWN'
                  ) AS customer_identifier,  -- Universal identifier
        customer_id,
        customer_email
    FROM {{ ref('dim_customers') }} 
),

final_payments AS (
    SELECT 
        p.payment_id::VARCHAR AS payment_id,  -- Primary key 
        c.customer_identifier AS customer_identifier,  -- Foreign key
        p.currency_code::VARCHAR AS currency_code,
        p.created_at::TIMESTAMP AS payment_creation_date,
        p.updated_at::TIMESTAMP AS payment_update_date,
        p.token_id::VARCHAR AS payment_instrument_token_id,  -- Foreign key
        p.vaulted_token_id::VARCHAR AS vaulted_token_id,
        p.merchant_payment_id::VARCHAR AS merchant_payment_id,
        p.primer_account_id::VARCHAR AS primer_account_id,  -- Foreign key
        p.amount::INTEGER AS payment_amount,
        p.status::VARCHAR AS payment_status,
        p.processor_merchant_id::VARCHAR AS processor_merchant_id,
        p.processor::VARCHAR AS processor_name,
        p.amount_captured::INTEGER AS amount_captured,
        p.amount_authorized::INTEGER AS amount_authorized,
        p.amount_refunded::INTEGER AS amount_refunded,

        -- Extract Purchase Descriptor (Extract last word after colon)
        CASE 
            WHEN p.statement_descriptor LIKE '%: %' 
            THEN TRIM(SPLIT_PART(p.statement_descriptor, ':', 2)) 
            ELSE p.statement_descriptor 
        END AS purchase_descriptor,

        -- Derived Metrics
        p.amount - COALESCE(p.amount_refunded, 0) AS net_amount,

        NOW()::TIMESTAMP AS ingestion_timestamp,
        ROW_NUMBER() OVER (PARTITION BY p.payment_id ORDER BY p.created_at DESC) AS row_num --- Row number for deduplication

    FROM staged_payments p
    LEFT JOIN linked_customers c 
         ON COALESCE(
                     json_extract_string(p.cleaned_customer_details, '$.customer_id'), 
                     json_extract_string(p.cleaned_customer_details, '$.email_address')
                    ) = c.customer_identifier

),

deduplicated_payments AS (
    SELECT *
    FROM final_payments
    WHERE row_num = 1 --- keeping only the most recent record

)

SELECT * FROM deduplicated_payments 

{% if is_incremental() %}
WHERE created_at > COALESCE((SELECT MAX(created_at) FROM {{ this }}), '1900-01-01')
{% endif %}
