BEGIN;

CREATE TABLE IF NOT EXISTS public.date_processing_status
(
    date date NOT NULL,
    status character varying(20) COLLATE pg_catalog."default" NOT NULL,
    last_checked timestamp without time zone DEFAULT now(),
    retry_count integer DEFAULT 0,
    CONSTRAINT date_processing_status_pkey PRIMARY KEY (date),
    CONSTRAINT date_processing_status_status_check CHECK (status::text = ANY (ARRAY['pending'::character varying, 'processed'::character varying, 'no_games'::character varying]::text[]))
)

CREATE TABLE IF NOT EXISTS public.api_call_logs
(
    id serial NOT NULL,
    endpoint text COLLATE pg_catalog."default",
    call_time timestamp without time zone,
    success boolean,
    error_message text COLLATE pg_catalog."default",
    CONSTRAINT api_call_logs_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.boxscoretraditionalv2__playerstats
(
    id serial NOT NULL,
    "GAME_ID" text COLLATE pg_catalog."default",
    "TEAM_ID" bigint,
    "TEAM_ABBREVIATION" text COLLATE pg_catalog."default",
    "TEAM_CITY" text COLLATE pg_catalog."default",
    "PLAYER_ID" bigint,
    "PLAYER_NAME" text COLLATE pg_catalog."default",
    "NICKNAME" text COLLATE pg_catalog."default",
    "START_POSITION" text COLLATE pg_catalog."default",
    "COMMENT" text COLLATE pg_catalog."default",
    "MIN" text COLLATE pg_catalog."default",
    "FGM" bigint,
    "FGA" bigint,
    "FG_PCT" double precision,
    "FG3M" bigint,
    "FG3A" bigint,
    "FG3_PCT" double precision,
    "FTM" bigint,
    "FTA" bigint,
    "FT_PCT" double precision,
    "OREB" bigint,
    "DREB" bigint,
    "REB" bigint,
    "AST" bigint,
    "STL" bigint,
    "BLK" bigint,
    "TO" bigint,
    "PF" bigint,
    "PTS" bigint,
    "PLUS_MINUS" double precision,
    updated_at timestamp without time zone,
    CONSTRAINT boxscoretraditionalv2__playerstats_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.boxscoretraditionalv2__teamstarterbenchstats
(
    id serial NOT NULL,
    "GAME_ID" text COLLATE pg_catalog."default",
    "TEAM_ID" bigint,
    "TEAM_NAME" text COLLATE pg_catalog."default",
    "TEAM_ABBREVIATION" text COLLATE pg_catalog."default",
    "TEAM_CITY" text COLLATE pg_catalog."default",
    "STARTERS_BENCH" text COLLATE pg_catalog."default",
    "MIN" text COLLATE pg_catalog."default",
    "FGM" bigint,
    "FGA" bigint,
    "FG_PCT" double precision,
    "FG3M" bigint,
    "FG3A" bigint,
    "FG3_PCT" double precision,
    "FTM" bigint,
    "FTA" bigint,
    "FT_PCT" double precision,
    "OREB" bigint,
    "DREB" bigint,
    "REB" bigint,
    "AST" bigint,
    "STL" bigint,
    "BLK" bigint,
    "TO" bigint,
    "PF" bigint,
    "PTS" bigint,
    updated_at timestamp without time zone,
    CONSTRAINT boxscoretraditionalv2__teamstarterbenchstats_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.boxscoretraditionalv2__teamstats
(
    id serial NOT NULL,
    "GAME_ID" text COLLATE pg_catalog."default",
    "TEAM_ID" bigint,
    "TEAM_NAME" text COLLATE pg_catalog."default",
    "TEAM_ABBREVIATION" text COLLATE pg_catalog."default",
    "TEAM_CITY" text COLLATE pg_catalog."default",
    "MIN" text COLLATE pg_catalog."default",
    "FGM" bigint,
    "FGA" bigint,
    "FG_PCT" double precision,
    "FG3M" bigint,
    "FG3A" bigint,
    "FG3_PCT" double precision,
    "FTM" bigint,
    "FTA" bigint,
    "FT_PCT" double precision,
    "OREB" bigint,
    "DREB" bigint,
    "REB" bigint,
    "AST" bigint,
    "STL" bigint,
    "BLK" bigint,
    "TO" bigint,
    "PF" bigint,
    "PTS" bigint,
    "PLUS_MINUS" double precision,
    updated_at timestamp without time zone,
    CONSTRAINT boxscoretraditionalv2__teamstats_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.leaguegamefinder__leaguegamefinderresults
(
    id serial NOT NULL,
    "SEASON_ID" text COLLATE pg_catalog."default",
    "TEAM_ID" bigint,
    "TEAM_ABBREVIATION" text COLLATE pg_catalog."default",
    "TEAM_NAME" text COLLATE pg_catalog."default",
    "GAME_ID" text COLLATE pg_catalog."default",
    "GAME_DATE" text COLLATE pg_catalog."default",
    "MATCHUP" text COLLATE pg_catalog."default",
    "WL" text COLLATE pg_catalog."default",
    "MIN" bigint,
    "PTS" bigint,
    "FGM" bigint,
    "FGA" bigint,
    "FG_PCT" double precision,
    "FG3M" bigint,
    "FG3A" bigint,
    "FG3_PCT" double precision,
    "FTM" bigint,
    "FTA" bigint,
    "FT_PCT" double precision,
    "OREB" bigint,
    "DREB" bigint,
    "REB" bigint,
    "AST" bigint,
    "STL" bigint,
    "BLK" bigint,
    "TOV" bigint,
    "PF" bigint,
    "PLUS_MINUS" double precision,
    updated_at timestamp without time zone,
    CONSTRAINT leaguegamefinder__leaguegamefinderresults_pkey PRIMARY KEY (id)
);
END;