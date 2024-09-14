CREATE TABLE IF NOT EXISTS games (
  game_id VARCHAR PRIMARY KEY,
  home_team VARCHAR,
  away_team VARCHAR,
  date DATE,
  status VARCHAR
);

CREATE TABLE IF NOT EXISTS boxscores (
  game_id VARCHAR PRIMARY KEY,
  points_home INT,
  points_away INT,
  rebounds_home INT,
  rebounds_away INT,
  assists_home INT,
  assists_away INT
);
