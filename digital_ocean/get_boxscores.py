import logging
from typing import List
from nba_api.stats.endpoints.boxscoretraditionalv2 import BoxScoreTraditionalV2

from helper import log_api_call, upsert_all_data_sets, db_connection, get_distinct_game_ids

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MAX_BOXSCORES_PER_RUN = 5


def fetch_boxscores(db, game_ids: List[str], batch_size: int = 10):
    for i in range(0, len(game_ids), batch_size):
        batch = game_ids[i:i + batch_size]
        for game_id in batch:
            try:
                boxscore = BoxScoreTraditionalV2(game_id=game_id)
                upsert_all_data_sets(db, boxscore)
                log_api_call(db, "BoxScoreTraditionalV2", True)
                logger.info(f"Successfully fetched boxscore for game {game_id}")
            except Exception as e:
                log_api_call(db, "BoxScoreTraditionalV2", False, str(e))
                logger.error(f"Error fetching boxscore for game {game_id}: {e}")

def get_unfetched_boxscores():
    with db_connection() as db:
        all_game_ids = get_distinct_game_ids(db, 'leaguegamefinder__leaguegamefinderresults')
        existing_boxscore_game_ids = get_distinct_game_ids(db, 'boxscoretraditionalv2__teamstats')
        unfetched_game_ids = list(all_game_ids - existing_boxscore_game_ids)

        logger.info(f"Total games: {len(all_game_ids)}, Unfetched games: {len(unfetched_game_ids)}")

        if unfetched_game_ids:
            # Limit the number of boxscores to fetch in this run
            limited_unfetched_game_ids = unfetched_game_ids[:MAX_BOXSCORES_PER_RUN]
            logger.info(f"Fetching {len(limited_unfetched_game_ids)} boxscores in this run")
            fetch_boxscores(db, limited_unfetched_game_ids)
        else:
            logger.info("No new boxscores to fetch.")

if __name__ == "__main__":
    get_unfetched_boxscores()
