{{
    config(
        materialized='incremental',
        unique_key='game_date',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

WITH game_boxscore_status AS (
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
        GREATEST(gf.updated_at, COALESCE(bp.updated_at, gf.updated_at)) as updated_at
    FROM game_finder gf
    LEFT JOIN boxscore_presence bp
        ON gf.game_id = bp.game_id
)

SELECT
    game_date,
    COUNT(DISTINCT game_id) as games_count,
    COUNT(DISTINCT CASE WHEN has_boxscore THEN game_id END) as boxscores_count,
    MAX(updated_at) as updated_at
FROM game_boxscore_status
GROUP BY game_date
{% if is_incremental() %}
HAVING MAX(updated_at) >= (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}