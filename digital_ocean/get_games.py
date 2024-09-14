import dataset
from nba_api.stats.endpoints import LeagueGameFinder
from datetime import datetime

# Database connection using dataset
db = dataset.connect("postgresql://nbauser:nbapass@localhost:5432/nba_db")
games_table = db["games"]

def get_games(start_date, end_date):
    # Fetch NBA games for the specified date range
    gamefinder = LeagueGameFinder(date_from_nullable=start_date, date_to_nullable=end_date)
    games = gamefinder.get_dict()["resultSets"][0]["rowSet"]

    for game in games:
        game_id = game[2]
        home_team = game[6]
        away_team = game[7]
        date = game[0]
        status = game[8]

        # Upsert into the database (insert if not exists, otherwise update)
        games_table.upsert({
            "game_id": game_id,
            "home_team": home_team,
            "away_team": away_team,
            "date": date,
            "status": status
        }, ["game_id"])

if __name__ == "__main__":
    # Example date range
    get_games("2024-01-01", "2024-01-31")
