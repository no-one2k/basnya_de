{{
    config(
        materialized='view'
    )
}}

SELECT
    "GAME_ID" AS game_id,
    TO_DATE("GAME_DATE", 'YYYY-MM-DD') AS game_date,
    "TEAM_ID",
    "TEAM_ABBREVIATION",
    "WL" AS result
FROM {{ source('nba_source', 'leaguegamefinder__leaguegamefinderresults') }}