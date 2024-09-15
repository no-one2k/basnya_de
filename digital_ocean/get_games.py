import dataset
from dataset import Database, Table
from nba_api.stats.endpoints import LeagueGameFinder
from datetime import datetime

from digital_ocean.helper import log_api_call, make_raw_data_table_name, upsert_all_data_sets

# Database connection using dataset
db: Database = dataset.connect("postgresql://nbauser:nbapass@localhost:5432/nba_db")


def get_games(start_date, end_date):
    try:
        # Fetch NBA games for the specified date range
        gamefinder = LeagueGameFinder(date_from_nullable=start_date, date_to_nullable=end_date)
        upsert_all_data_sets(db, gamefinder)

        log_api_call(db,"LeagueGameFinder", True)
    except Exception as e:
        # Log the failed API call with the error message
        log_api_call(db, "LeagueGameFinder", False, str(e))
        print(f"Error fetching games: {e}")

if __name__ == "__main__":
    # Example date range
    get_games("2024-01-01", "2024-01-31")
