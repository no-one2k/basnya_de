{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}

{% set recency_window_days = 10 %}

{% set start_date = '2025-04-01' %}
{% set max_rank_to_track = 20 %}

-- Daily Top-20 Players by metric snapshot per processed date
-- Grain after change: (cutoff_date, season_id, metric_name, rank)

with processed_dates as (
    select game_date as cutoff_date
    from {{ ref('stg_date_processing_status') }}
    where status = 'processed'
      and game_date >= '{{ start_date }}'
      {% if is_incremental() %}
        and game_date not in (select distinct cutoff_date from {{ this }})
      {% endif %}
),

-- Canonicalize one row per (GAME_ID, PLAYER_ID) from player boxscores
pgs_ranked as (
    select
        bp.id,
        bp."GAME_ID",
        bp."PLAYER_ID",
        bp."TEAM_ID",
        bp."PLAYER_NAME",
        bp."PTS",
        bp."MIN",
        bp."AST",
        bp."REB",
        bp."DREB",
        bp."OREB",
        bp."STL",
        bp."BLK",
        bp."PF",
        bp."FGM",
        bp."FGA",
        bp."FG3M",
        bp."FG3A",
        bp."FTM",
        bp."FTA",
        bp.updated_at,
        row_number() over (
            partition by bp."GAME_ID", bp."PLAYER_ID"
            order by
                coalesce(bp.updated_at, timestamp '1970-01-01') desc,
                bp.id desc
        ) as rn
    from {{ source('nba_source', 'boxscoretraditionalv2__playerstats') }} bp
    where bp."GAME_ID" is not null
      and bp."PLAYER_ID" is not null
),
pgs_canonical as (
    select
        "PLAYER_ID",
        "PLAYER_NAME",
        "TEAM_ID",
        "GAME_ID",
        "PTS",
        "MIN",
        "AST",
        "REB",
        "DREB",
        "OREB",
        "STL",
        "BLK",
        "PF",
        "FGM",
        "FGA",
        "FG3M",
        "FG3A",
        "FTM",
        "FTA"
    from pgs_ranked
    where rn = 1
),

-- Player game stats filtered to rows where the player actually played
player_games as (
    select
        pgs."PLAYER_ID"    as player_id,
        pgs."PLAYER_NAME"  as player_name,
        pgs."TEAM_ID"      as team_id,
        pgs."GAME_ID"      as game_id,
        pgs."PTS"::numeric as pts,
        pgs."AST"::numeric as ast,
        pgs."REB"::numeric as reb,
        pgs."DREB"::numeric as dreb,
        pgs."OREB"::numeric as oreb,
        pgs."STL"::numeric as stl,
        pgs."BLK"::numeric as blk,
        pgs."PF"::numeric  as pf,
        pgs."FGM"::numeric as fgm,
        pgs."FGA"::numeric as fga,
        pgs."FG3M"::numeric as fg3m,
        pgs."FG3A"::numeric as fg3a,
        pgs."FTM"::numeric as ftm,
        pgs."FTA"::numeric as fta,
        pgs."MIN"          as min_str,
        -- Derived per-game fields
        (pgs."FGM"::numeric - pgs."FG3M"::numeric) as fg2m,
        (pgs."FGA"::numeric - pgs."FG3A"::numeric) as fg2a,
        -- Minutes in decimal (e.g., 34:30 -> 34.5)
        (split_part(pgs."MIN", ':', 1)::numeric + (split_part(pgs."MIN", ':', 2)::numeric / 60)) as minutes_dec,
        -- Double/double-double/triple-double flags per game based on 10+ in PTS, REB, AST, STL, BLK
        (
            ((case when pgs."PTS"::numeric >= 10 then 1 else 0 end)
           + (case when pgs."REB"::numeric >= 10 then 1 else 0 end)
           + (case when pgs."AST"::numeric >= 10 then 1 else 0 end)
           + (case when pgs."STL"::numeric >= 10 then 1 else 0 end)
           + (case when pgs."BLK"::numeric >= 10 then 1 else 0 end))
        ) as ten_plus_count
    from pgs_canonical pgs
    where pgs."PTS" is not null
      and pgs."MIN" is not null
      and pgs."MIN" <> '0:00'
),

-- Canonicalize one row per GAME_ID from leaguegamefinder
lgf_ranked as (
    select
        lgf."GAME_ID",
        lgf."SEASON_ID",
        lgf."GAME_DATE",
        lgf.updated_at,
        -- Prefer official competitions (2=Regular, 4=Playoffs, 5=Play-In)
        case substring(lgf."SEASON_ID"::text from 1 for 1)
            when '2' then 0
            when '4' then 0
            when '5' then 0
            else 1
        end as non_official_flag,
        row_number() over (
            partition by lgf."GAME_ID"
            order by
                case substring(lgf."SEASON_ID"::text from 1 for 1)
                    when '2' then 0
                    when '4' then 0
                    when '5' then 0
                    else 1
                end asc,
                coalesce(lgf.updated_at, timestamp '1970-01-01') desc,
                lgf."SEASON_ID" desc
        ) as rn
    from {{ source('nba_source', 'leaguegamefinder__leaguegamefinderresults') }} lgf
),
lgf_canonical as (
    select
        "GAME_ID",
        "SEASON_ID",
        "GAME_DATE",
        updated_at
    from lgf_ranked
    where rn = 1
),

latest_season_game_dates as (
    select lgf."SEASON_ID", max(lgf."GAME_DATE") as latest_game_date
    from lgf_canonical lgf
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
        pg.pts,
        pg.ast,
        pg.reb,
        pg.dreb,
        pg.oreb,
        pg.stl,
        pg.blk,
        pg.pf,
        pg.fgm,
        pg.fga,
        pg.fg3m,
        pg.fg3a,
        pg.ftm,
        pg.fta,
        pg.fg2m,
        pg.fg2a,
        pg.minutes_dec,
        pg.ten_plus_count
    from player_games pg
    join lgf_canonical lgf
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
        pgm.latest_season_game_date,
        pgm.pts,
        pgm.ast,
        pgm.reb,
        pgm.dreb,
        pgm.oreb,
        pgm.stl,
        pgm.blk,
        pgm.pf,
        pgm.fgm,
        pgm.fga,
        pgm.fg3m,
        pgm.fg3a,
        pgm.ftm,
        pgm.fta,
        pgm.fg2m,
        pgm.fg2a,
        pgm.minutes_dec,
        pgm.ten_plus_count
    from player_games_with_meta pgm
    cross join processed_dates pd
    where pgm.game_date <= pd.cutoff_date
      and (
        (pd.cutoff_date >= (pgm.latest_season_game_date - interval '{{ recency_window_days }} days'))
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
        avg(pts) as avg_pts,
        avg(ast) as avg_ast,
        avg(reb) as avg_reb,
        avg(dreb) as avg_dreb,
        avg(oreb) as avg_oreb,
        avg(stl) as avg_stl,
        avg(blk) as avg_blk,
        avg(pf)  as avg_pf,
        avg(minutes_dec) as avg_minutes,
        avg(ftm) as avg_ftm,
        avg(fta) as avg_fta,
        avg(fg3m) as avg_fg3m,
        avg(fg3a) as avg_fg3a,
        avg(fg2m) as avg_fg2m,
        avg(fg2a) as avg_fg2a,
        avg(fgm) as avg_fgm,
        avg(fga) as avg_fga,
        sum(case when ten_plus_count >= 1 then 1 else 0 end) as doubles,
        sum(case when ten_plus_count >= 2 then 1 else 0 end) as double_doubles,
        sum(case when ten_plus_count >= 3 then 1 else 0 end) as triple_doubles,
        max(game_date) as last_player_game_date_in_scope,
        max(latest_season_game_date) as latest_season_game_date,
        (array_agg(team_id order by game_date desc, game_id desc))[1] as team_id
    from scoped_player_games
    group by 1,2,3
),

-- Unpivot metrics into rows for ranking
metrics_unioned as (
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_pts' as metric_name, avg_pts as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_ast' as metric_name, avg_ast as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_reb' as metric_name, avg_reb as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_dreb' as metric_name, avg_dreb as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_oreb' as metric_name, avg_oreb as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_stl' as metric_name, avg_stl as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_blk' as metric_name, avg_blk as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_minutes' as metric_name, avg_minutes as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_pf' as metric_name, avg_pf as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_ftm' as metric_name, avg_ftm as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_fta' as metric_name, avg_fta as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_fg2m' as metric_name, avg_fg2m as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_fg2a' as metric_name, avg_fg2a as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_fg3m' as metric_name, avg_fg3m as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_fg3a' as metric_name, avg_fg3a as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_fgm' as metric_name, avg_fgm as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'avg_fga' as metric_name, avg_fga as metric_value from player_aggregates
    union all
    -- counts of doubles
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'doubles' as metric_name, doubles::numeric as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'double_doubles' as metric_name, double_doubles::numeric as metric_value from player_aggregates
    union all
    select cutoff_date, season_id, player_id, player_name, team_id, games_played, last_player_game_date_in_scope, latest_season_game_date,
           'triple_doubles' as metric_name, triple_doubles::numeric as metric_value from player_aggregates
),

-- Rank players per (cutoff_date, season_id, metric_name)
ranked as (
    select
        mu.*,
        row_number() over (
            partition by mu.cutoff_date, mu.season_id, mu.metric_name
            order by mu.metric_value desc, mu.games_played desc, mu.player_id asc
        ) as rank
    from metrics_unioned mu
)

select
    cutoff_date::date as cutoff_date,
    season_id::text as season_id,
    metric_name,
    rank::int as rank,
    player_id,
    player_name,
    team_id,
    games_played::int as games_played,
    metric_value::numeric as metric_value,
    last_player_game_date_in_scope::date as last_player_game_date_in_scope,
    latest_season_game_date::date as latest_season_game_date,
    now() as updated_at
from ranked
where rank <= {{ max_rank_to_track }}
  and games_played > 0
  and latest_season_game_date >= (cutoff_date - interval '{{ recency_window_days }} days')
order by cutoff_date, season_id, metric_name, rank