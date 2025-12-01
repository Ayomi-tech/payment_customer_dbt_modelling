
{{ config(materialized='table') }}

WITH source AS (
    SELECT * 
    FROM {{ source('raw_data', 'payment_instrument_token_data') }}
),

cleaned_data AS (
    SELECT
        token_id :: VARCHAR AS token_id, --- Cast to VARCHAR
        token_type :: VARCHAR AS token_type ,
        payment_instrument_type :: VARCHAR AS payment_instrument_type,
        network :: VARCHAR network,
        NOW()::TIMESTAMP AS ingestion_timestamp,

        --- Fix 3D JSON Formatting: Replace single quotes and convert 'None' to null
        CASE 
            WHEN json_valid(
                REPLACE(
                    REPLACE(three_d_secure_authentication, '''', '"'), 
                    'None', 'null'
                )
            ) 
            THEN json(REPLACE(REPLACE(three_d_secure_authentication, '''', '"'), 'None', 'null')) 
            ELSE NULL 
        END AS cleaned_auth,

    FROM source
)

SELECT * FROM cleaned_data

{% if is_incremental() %}
WHERE ingestion_timestamp IS NOT NULL 
AND ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}
