{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}

{% set recency_window_days = 10 %}

{% set start_date = '2025-01-01' %}


-- Daily Top-20 Players by PPG snapshot per processed date
-- Grain: (cutoff_date, season_id, rank)

with processed_dates as (
    select game_date as cutoff_date
    from {{ ref('stg_date_processing_status') }}
    where status = 'processed'
    and game_date >= '{{ start_date }}'
    {% if is_incremental() %}
      and game_date not in (select distinct cutoff_date from {{ this }})
    {% endif %}
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

latest_season_game_dates as (
    select lgf."SEASON_ID", max(lgf."GAME_DATE") as latest_game_date
    from {{ source('nba_source', 'leaguegamefinder__leaguegamefinderresults') }} lgf
    group by 1
),

-- Join player boxscores to game metadata; use this as the authoritative game universe
player_games_with_meta as (
    select
        pg.player_id,
        pg.player_name,
        pg.team_id,
        pg.game_id,
        lgf."SEASON_ID"    as season_id,
        to_date(lgf."GAME_DATE", 'YYYY-MM-DD') as game_date,
        to_date(lsgd.latest_game_date, 'YYYY-MM-DD') as latest_season_game_date,
        pg.pts
    from player_games pg
    join {{ source('nba_source', 'leaguegamefinder__leaguegamefinderresults') }} lgf
      on lgf."GAME_ID" = pg.game_id
    join latest_season_game_dates lsgd
      on lsgd."SEASON_ID" = lgf."SEASON_ID"
),

-- Scope player games to the relevant season and up to the cutoff_date for each snapshot
scoped_player_games as (
    select
        pd.cutoff_date,
        pgm.season_id,
        pgm.player_id,
        pgm.player_name,
        pgm.team_id,
        pgm.game_id,
        pgm.game_date,
        pgm.pts
        pgm.latest_season_game_date
    from player_games_with_meta pgm
    cross join processed_dates pd
    where pgm.game_date <= pd.cutoff_date
    and ((pd.cutoff_date >= (pgm.latest_season_game_date - interval '{{ recency_window_days }} days'))
        or (pd.cutoff_date between (pgm.latest_season_game_date - interval '365 days') and (pgm.latest_season_game_date - interval '1 days'))
        )
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
        max(game_date) as last_player_game_date_in_scope,
        latest_season_game_date,
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
    last_player_game_date_in_scope::date as last_player_game_date_in_scope,
    latest_season_game_date::date as latest_season_game_date
    now() as updated_at
from ranked
where rank <= 20
and games_played > 0
and latest_season_game_date >= (cutoff_date - interval '{{ recency_window_days }} days')
order by cutoff_date, season_id, rank