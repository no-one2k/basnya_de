{{
    config(
        materialized='incremental',
        unique_key='game_date',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

SELECT
    game_date,
    COUNT(DISTINCT game_id) as total_games,
    COUNT(DISTINCT CASE WHEN has_boxscore THEN game_id END) as games_with_boxscore,
    MAX(updated_at) as updated_at
FROM {{ ref('stg_game_boxscore_status') }}
{% if is_incremental() %}
WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
GROUP BY game_date