{{ config(
    materialized='incremental',
    unique_key='season_id',
    on_schema_change='sync_all_columns'
) }}

with games as (
    select
        -- raw season_id from NBA API (e.g., '22023')
        lgf."SEASON_ID"::text as season_id,
        -- first digit (season_part) and human-friendly name
        substring(lgf."SEASON_ID" from 1 for 1) as season_part,
        case substring(lgf."SEASON_ID" from 1 for 1)
            when '1' then 'Preseason'
            when '2' then 'Regular Season'
            when '3' then 'All-Star'
            when '4' then 'Playoffs'
            when '5' then 'Play-In'
            when '6' then 'NBA Cup Final'
            else 'Unknown'
        end as season_part_name,
        -- numeric start year (YYYY) portion
        nullif(substring(lgf."SEASON_ID" from 2), '')::int as start_year,
        lgf."GAME_ID"::text as game_id,
        -- attempt to cast to date; if already a date-compatible text it will work
        cast(lgf."GAME_DATE" as date) as game_date,
        lgf.updated_at
    from public.leaguegamefinder__leaguegamefinderresults lgf
    where lgf."SEASON_ID" is not null
),
-- boxscores presence by game (use player boxscores as a proxy for a loaded boxscore)
boxscores as (
    select
        bp."GAME_ID"::text as game_id,
        max(bp.updated_at) as updated_at
    from public.boxscoretraditionalv2__playerstats bp
    where bp."GAME_ID" is not null
    group by bp."GAME_ID"
),
season_rollup as (
    select
        g.season_id,
        g.start_year,
        g.season_part,
        g.season_part_name,
        min(g.game_date) as first_game_date,
        max(g.game_date) as last_game_date,
        count(distinct g.game_id) as found_games,
        count(distinct case when b.game_id is not null then g.game_id end) as loaded_boxscores,
        greatest(
            max(g.updated_at),
            coalesce(max(b.updated_at), timestamp '1970-01-01')
        ) as latest_update
    from games g
    left join boxscores b
        on b.game_id = g.game_id
    group by
        g.season_id,
        g.start_year,
        g.season_part,
        g.season_part_name
)

select
    season_id,
    start_year,
    season_part,
    season_part_name,
    first_game_date,
    last_game_date,
    found_games,
    loaded_boxscores,
    latest_update
from season_rollup