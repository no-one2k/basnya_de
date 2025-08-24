{{
    config(
        materialized='table'
    )
}}

-- Daily Top-20 Players by PPG snapshot per processed date
-- Grain: (cutoff_date, season_id, rank)

with processed_dates as (
    select game_date as cutoff_date
    from {{ ref('stg_date_processing_status') }}
    where status = 'processed'
),

-- Games with parsed dates and season start year for ordering
games as (
    select
        "GAME_ID"      as game_id,
        to_date("GAME_DATE", 'YYYY-MM-DD') as game_date,
        "SEASON_ID"    as season_id,
        substring("SEASON_ID", 1, 4)::int as season_start_year
    from {{ source('nba_source', 'leaguegamefinder__leaguegamefinderresults') }}
),

-- Determine current season as of each processed cutoff_date (latest season observed up to that date)
current_season_by_date as (
    select
        pd.cutoff_date,
        max(g.season_start_year) as season_start_year
    from processed_dates pd
    join games g
      on g.game_date <= pd.cutoff_date
    group by pd.cutoff_date
),

-- Resolve season_id text for each cutoff_date from the max season_start_year
season_id_by_date as (
    select
        cs.cutoff_date,
        -- choose the max season_id text among games of that start year to have a stable text value
        max(g.season_id) as season_id
    from current_season_by_date cs
    join games g
      on g.season_start_year = cs.season_start_year
     and g.game_date <= cs.cutoff_date
    group by cs.cutoff_date
),

-- Player game stats filtered to rows where the player actually played
player_games as (
    select
        pgs."PLAYER_ID"   as player_id,
        pgs."PLAYER_NAME" as player_name,
        pgs."TEAM_ID"     as team_id,
        pgs."GAME_ID"     as game_id,
        pgs."PTS"::numeric as pts,
        pgs."MIN"         as min_str
    from {{ source('nba_source', 'boxscoretraditionalv2__playerstats') }} pgs
    where pgs."PTS" is not null
      and pgs."MIN" is not null
      and pgs."MIN" <> '0:00'
),

-- Scope player games to the relevant season and up to the cutoff_date for each snapshot
scoped_player_games as (
    select
        sid.cutoff_date,
        sid.season_id,
        pg.player_id,
        pg.player_name,
        pg.team_id,
        g.game_id,
        g.game_date,
        pg.pts
    from season_id_by_date sid
    join games g
      on g.season_id = sid.season_id
     and g.game_date <= sid.cutoff_date
    join player_games pg
      on pg.game_id = g.game_id
),

-- Aggregate per player within the scoped period
player_aggregates as (
    select
        cutoff_date,
        season_id,
        player_id,
        min(player_name) as player_name,
        count(distinct game_id) as games_played,
        avg(pts::numeric) as avg_pts,
        max(game_date) as last_game_date_in_scope,
        -- pick the latest team_id based on most recent game_date and then deterministic by game_id
        (array_agg(team_id order by game_date desc, game_id desc))[1] as team_id
    from scoped_player_games
    group by 1,2,3
),

-- Rank players per (cutoff_date, season_id)
ranked as (
    select
        *,
        row_number() over (
            partition by cutoff_date, season_id
            order by avg_pts desc, games_played desc, player_id asc
        ) as rank
    from player_aggregates
)

select
    cutoff_date::date as cutoff_date,
    season_id::text as season_id,
    rank::int as rank,
    player_id,
    player_name,
    team_id,
    games_played::int as games_played,
    avg_pts::numeric as avg_pts,
    last_game_date_in_scope::date as last_game_date_in_scope,
    now() as updated_at
from ranked
where rank <= 20
order by cutoff_date, season_id, rank
