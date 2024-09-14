import dataset
from nba_api.stats.endpoints import BoxScoreTraditionalV2

# Database connection using dataset
db = dataset.connect("postgresql://nbauser:nbapass@localhost:5432/nba_db")
games_table = db["games"]
boxscores_table = db["boxscores"]

def get_unfetched_boxscores():
    games = games_table.find(status="Final")  # Assuming "Final" means the game has concluded

    for game in games:
        game_id = game["game_id"]

        # Check if boxscore is already fetched
        if not boxscores_table.find_one(game_id=game_id):
            boxscore = BoxScoreTraditionalV2(game_id=game_id).get_dict()["resultSets"][0]["rowSet"][0]
            points_home = boxscore[22]  # Home points
            points_away = boxscore[23]  # Away points
            rebounds_home = boxscore[18]
            rebounds_away = boxscore[19]
            assists_home = boxscore[20]
            assists_away = boxscore[21]

            # Upsert into the database
            boxscores_table.upsert({
                "game_id": game_id,
                "points_home": points_home,
                "points_away": points_away,
                "rebounds_home": rebounds_home,
                "rebounds_away": rebounds_away,
                "assists_home": assists_home,
                "assists_away": assists_away
            }, ["game_id"])

if __name__ == "__main__":
    get_unfetched_boxscores()
