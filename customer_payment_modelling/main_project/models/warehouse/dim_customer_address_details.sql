{{ config(materialized='incremental', unique_key='address_id', incremetal ='merge') }}

WITH staged_payments AS (
    SELECT * FROM {{ ref('stg_payment') }}
),

cleaned_data AS (
    SELECT
        payment_id,
        created_at,
        updated_at,
        COALESCE(
            REPLACE(REPLACE(NULLIF(cleaned_customer_details, ''), '''', '"'), 'None', 'null'),
            '{}'
        ) AS final_customer_details
    FROM staged_payments
),

extracted_addresses AS (
    -- Billing Address Extraction
    SELECT
           UUID() AS address_id, --- Generated UUID for billing address
        
        -- Allow NULL customer_id and use Email as an alternative identifier
        COALESCE(
            json_extract_string(c.final_customer_details, '$.customer_id'), 
            json_extract_string(c.final_customer_details, '$.email_address'),
            'UNKNOWN' --- Default value if both are NULL
        ) AS customer_identifier,
        'billing' AS address_type,
        json_extract_string(c.final_customer_details, '$.billing_address.first_name') AS first_name,
        json_extract_string(c.final_customer_details, '$.billing_address.last_name') AS last_name,
        json_extract_string(c.final_customer_details, '$.billing_address.email') AS email,
        json_extract_string(c.final_customer_details, '$.billing_address.city') AS city,
        json_extract_string(c.final_customer_details, '$.billing_address.state') AS state,
        json_extract_string(c.final_customer_details, '$.billing_address.country_code') AS country_code,
        json_extract_string(c.final_customer_details, '$.billing_address.postal_code') AS postal_code,
        json_extract_string(c.final_customer_details, '$.billing_address.address_line_1') AS address_line_1,
        json_extract_string(c.final_customer_details, '$.billing_address.address_line_2') AS address_line_2,
        json_extract_string(c.final_customer_details, '$.billing_address.address_line_3') AS address_line_3,
        c.created_at,
        c.updated_at,
    FROM cleaned_data c
    WHERE json_valid(c.final_customer_details)
      AND json_extract_string(c.final_customer_details, '$.billing_address.address_line_1') IS NOT NULL

    UNION ALL

    --- Shipping Address Extraction
    SELECT
           UUID() AS address_id, --- Generated UUID for shipping address

        --- Allowing NULL customer_id and use email as an alternative identifier
        COALESCE(
            json_extract_string(c.final_customer_details, '$.customer_id'), 
            json_extract_string(c.final_customer_details, '$.email_address'),
            'UNKNOWN' --- Default value if both are NULL
        ) AS customer_identifier,
        'shipping' AS address_type,
        json_extract_string(c.final_customer_details, '$.shipping_address.first_name') AS first_name,
        json_extract_string(c.final_customer_details, '$.shipping_address.last_name') AS last_name,
        json_extract_string(c.final_customer_details, '$.shipping_address.email') AS email,
        json_extract_string(c.final_customer_details, '$.shipping_address.city') AS city,
        json_extract_string(c.final_customer_details, '$.shipping_address.state') AS state,
        json_extract_string(c.final_customer_details, '$.shipping_address.country_code') AS country_code,
        json_extract_string(c.final_customer_details, '$.shipping_address.postal_code') AS postal_code,
        json_extract_string(c.final_customer_details, '$.shipping_address.address_line_1') AS address_line_1,
        json_extract_string(c.final_customer_details, '$.shipping_address.address_line_2') AS address_line_2,
        json_extract_string(c.final_customer_details, '$.shipping_address.address_line_3') AS address_line_3,
        c.created_at,
        c.updated_at
    FROM cleaned_data c
    WHERE json_valid(c.final_customer_details)
      AND json_extract_string(c.final_customer_details, '$.shipping_address.address_line_1') IS NOT NULL
),

final_addresses AS (
  SELECT
         address_id::VARCHAR AS address_id, --- Primary key
         customer_identifier::VARCHAR AS customer_identifier, --- Foreign key
         address_type::VARCHAR AS address_type,
         first_name::VARCHAR AS first_name,
         last_name::VARCHAR AS last_name,
         email::VARCHAR AS email,
         city::VARCHAR AS city,
         country_code::VARCHAR AS country_code,
         postal_code::VARCHAR AS postal_code,
         address_line_1::VARCHAR AS address_line_1,
         address_line_2::VARCHAR AS address_line_2,
         address_line_3::VARCHAR AS address_line_3,
         created_at::TIMESTAMP AS address_creation_date,
         updated_at::TIMESTAMP AS address_updated_date,
         NOW()::TIMESTAMP AS ingestion_timestamp

  FROM extracted_addresses
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_identifier, address_type ORDER BY created_at DESC) = 1
)

SELECT * FROM final_addresses

{% if is_incremental() %}
WHERE address_creation_date IS NOT NULL
AND address_creation_date > COALESCE((SELECT MAX(address_creation_date) FROM {{ this }}), '1900-01-01')
{% endif %}
