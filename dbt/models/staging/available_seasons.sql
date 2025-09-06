{{ config(
    materialized='table',
    unique_key='season_id',
    on_schema_change='sync_all_columns'
) }}

with games as (
    select
        s.season_id,
        substring(s.season_id from 1 for 1) as season_part,
        case substring(s.season_id from 1 for 1)
            when '1' then 'Preseason'
            when '2' then 'Regular Season'
            when '3' then 'All-Star'
            when '4' then 'Playoffs'
            when '5' then 'Play-In'
            when '6' then 'NBA Cup Final'
            else 'Unknown'
        end as season_part_name,
        nullif(substring(s.season_id from 2), '')::int as start_year,
        s.game_id,
        min(s.game_date) as game_date,
        max(s.updated_at) as updated_at
    from (
        select
            lgf."SEASON_ID"::text as season_id,
            lgf."GAME_ID"::text as game_id,
            cast(lgf."GAME_DATE" as date) as game_date,
            lgf.updated_at
        from public.leaguegamefinder__leaguegamefinderresults lgf
        where lgf."SEASON_ID" is not null
          and lgf."GAME_ID" is not null
    ) s
    group by s.season_id, s.game_id
),
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
    -- expected totals by season part where well-defined; otherwise null
    case season_part
        when '1' then found_games -- Preseason: use found games
        when '2' then 1230       -- Regular Season (typical league-wide total)
        when '3' then 1          -- All-Star Game
        when '4' then found_games -- Playoffs: use found games
        when '5' then 6          -- Play-In (max games)
        when '6' then 1          -- NBA Cup Final
        else null
    end as expected_games,
    case season_part
        when '1' then found_games
        when '2' then 1230
        when '3' then 1
        when '4' then found_games
        when '5' then 6
        when '6' then 1
        else null
    end - found_games as games_to_load,
    found_games - loaded_boxscores as missing_boxscores,
    latest_update
from season_rollup