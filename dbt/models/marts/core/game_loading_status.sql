{{
    config(
        materialized='table',
        unique_key='game_id'
    )
}}

WITH games AS (
    SELECT DISTINCT
        game_id,
        game_date
    FROM {{ ref('stg_gamefinder') }}
),

boxscores AS (
    SELECT 
        game_id,
        CASE WHEN boxscore_loaded_at IS NOT NULL THEN true ELSE false END AS has_boxscore,
        boxscore_loaded_at
    FROM {{ ref('stg_boxscores') }}
),

processing_status AS (
    SELECT
        game_date,
        retry_count,
        last_checked
    FROM {{ ref('stg_date_processing_status') }}
)

SELECT
    g.game_id,
    g.game_date,
    COALESCE(b.has_boxscore, false) AS has_boxscore,
    COALESCE(ps.retry_count, 0) AS load_attempts,
    b.boxscore_loaded_at,
    ps.last_checked AS last_processed_check
FROM games g
LEFT JOIN boxscores b ON g.game_id = b.game_id
LEFT JOIN processing_status ps ON g.game_date = ps.game_date