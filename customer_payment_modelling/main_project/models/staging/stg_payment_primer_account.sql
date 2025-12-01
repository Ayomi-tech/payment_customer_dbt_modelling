
{{ config(materialized='table') }}

WITH source AS (
    SELECT * 
    FROM {{ source('raw_data', 'primer_account') }}

)

SELECT
    primer_account_id::VARCHAR AS primer_account_id,
    company_name::VARCHAR AS company_name,
    created_at::TIMESTAMP AS created_at,
    NOW()::TIMESTAMP AS ingestion_timestamp
FROM source
