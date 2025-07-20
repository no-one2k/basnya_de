{{
    config(
        materialized='incremental',
        unique_key=['game_date', 'game_id'],
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

WITH boxscore_presence AS (
    SELECT DISTINCT
        "GAME_ID" as game_id,
        updated_at
    FROM {{ source('nba_source', 'boxscoretraditionalv2__teamstats') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
),

game_finder AS (
    SELECT DISTINCT
        "GAME_ID" as game_id,
        DATE("GAME_DATE") as game_date,
        updated_at
    FROM {{ source('nba_source', 'leaguegamefinder__leaguegamefinderresults') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
)

SELECT
    gf.game_date,
    gf.game_id,
    CASE
        WHEN bp.game_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END as has_boxscore,
    COALESCE(dps.retry_count, 0) as retry_count,
    GREATEST(gf.updated_at, bp.updated_at, dps.last_checked) as updated_at
FROM game_finder gf
LEFT JOIN boxscore_presence bp
    ON gf.game_id = bp.game_id
LEFT JOIN {{ source('nba_source', 'date_processing_status') }} dps
    ON gf.game_date = dps.date
{% if is_incremental() %}
WHERE
    gf.updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
    OR bp.updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
    OR dps.last_checked >= (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}