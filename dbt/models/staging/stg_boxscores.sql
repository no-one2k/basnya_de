{{
    config(
        materialized='view'
    )
}}

SELECT 
    "GAME_ID" AS game_id,
    MAX(updated_at) AS boxscore_loaded_at
FROM (
    SELECT "GAME_ID", updated_at FROM {{ source('nba_source', 'boxscoretraditionalv2__playerstats') }}
    UNION ALL
    SELECT "GAME_ID", updated_at FROM {{ source('nba_source', 'boxscoretraditionalv2__teamstarterbenchstats') }}
    UNION ALL
    SELECT "GAME_ID", updated_at FROM {{ source('nba_source', 'boxscoretraditionalv2__teamstats') }}
) combined
GROUP BY 1