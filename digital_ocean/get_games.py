import logging
from typing import Optional, Dict

from dataset import Database
from nba_api.stats.endpoints.leaguegamefinder import LeagueGameFinder, LeagueIDNullable

from helper import log_api_call, upsert_all_data_sets, db_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def fetch_nba_games(db: Database, start_date: str, end_date: str) -> Optional[Dict]:
    """Fetch NBA games for the specified date range."""
    try:
        game_finder = LeagueGameFinder(
            league_id_nullable=LeagueIDNullable.nba,
            date_from_nullable=start_date,
            date_to_nullable=end_date,
        )
        upsert_all_data_sets(db, game_finder)
        log_api_call(db, "LeagueGameFinder", True)
        return game_finder.get_normalized_dict()
    except Exception as e:
        logger.exception(f"Error fetching games: {e}")
        log_api_call(db, "LeagueGameFinder", False, str(e))
        return None


def get_games(start_date: str, end_date: str) -> None:
    """Main function to fetch and store NBA games."""
    with db_connection() as db:
        if games := fetch_nba_games(db, start_date, end_date):
            logger.info(f"Successfully fetched games from {start_date} to {end_date}: {len(games)}")
        else:
            logger.error(f"Failed to fetch games from {start_date} to {end_date}")


if __name__ == "__main__":
    get_games("03/01/2021", "03/02/2021")
