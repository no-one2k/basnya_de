{{
    config(
        materialized='view'
    )
}}

SELECT
    date AS game_date,
    status,
    last_checked,
    retry_count
FROM {{ source('nba_source', 'date_processing_status') }}