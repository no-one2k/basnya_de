import dataset
from nba_api.stats.endpoints import BoxScoreSummaryV2

from helper import log_api_call, upsert_all_data_sets

# Database connection using dataset
db = dataset.connect("postgresql://nbauser:nbapass@localhost:5432/nba_db")
games_table = db["leaguegamefinder__leaguegamefinderresults"]
bs_summaries_table = db["boxscoresummaryv2__gamesummary"]

def get_unfetched_bs_summaries():
    games = games_table.find()#status="Final")  # Assuming "Final" means the game has concluded

    for game in games:
        game_id = game["GAME_ID"]

        # Check if boxscore is already fetched
        if not bs_summaries_table.find_one(game_id=game_id):
            try:
                # Fetch boxscore data
                boxscore = BoxScoreSummaryV2(game_id=game_id)
                upsert_all_data_sets(db, boxscore)

                log_api_call(db,"BoxScoreSummaryV2", True)

            except Exception as e:
                # Log the failed API call with the error message
                log_api_call(db,"BoxScoreSummaryV2", False, str(e))
                print(f"Error fetching boxscore for game {game_id}: {e}")


if __name__ == "__main__":
    get_unfetched_bs_summaries()
