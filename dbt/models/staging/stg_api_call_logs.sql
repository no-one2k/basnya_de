{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

SELECT
    id,
    endpoint,
    call_time,
    DATE(call_time) as call_date,
    success,
    error_message,
    call_time as updated_at
FROM {{ source('nba_source', 'api_call_logs') }}
{% if is_incremental() %}
WHERE call_time >= (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}