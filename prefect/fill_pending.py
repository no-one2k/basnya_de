#!/usr/bin/env python3
from datetime import timedelta
from typing import Optional, Dict

from dataset import Database
from nba_api.stats.endpoints import BoxScoreTraditionalV2
from prefect import flow, task, get_run_logger
from prefect.variables import Variable
from prefect.cache_policies import TASK_SOURCE, INPUTS

from track_processed_dates import update_processing_status, get_pending_dates

from get_games import fetch_nba_games

from helper import db_connection, convert_to_datetime, upsert_all_data_sets, log_api_call


@task(
    retries=10,
    persist_result=True,
    cache_expiration=timedelta(days=5),
    cache_policy=TASK_SOURCE+INPUTS - 'proxy'
)
def fetch_single_box_score(game_id: str, proxy=None):
    result = BoxScoreTraditionalV2(game_id=game_id, proxy=proxy)
    if not result.get_normalized_dict():
        raise ValueError(f"empty box score for {game_id}")
    return result

@task(
    retries=10,
    persist_result=True,
    cache_expiration=timedelta(days=5),
    cache_policy=TASK_SOURCE+INPUTS - 'db' - 'logger' - 'proxy')
def fetch_nba_games_task(db: Database, start_date: str, end_date: str, logger, proxy=None) -> Optional[Dict]:
    return fetch_nba_games(db=db, start_date=start_date, end_date=end_date, logger=logger, proxy=proxy)

@task(retries=2)
def process_single_date(db, dt_obj, logger, proxy=None):
    """
    Processes NBA game data for a single date:
      1. Marks the date as 'processing'.
      2. Fetches game data for the date using fetch_nba_games (with the start and end date as the same day).
      3. Extracts game IDs (if any) and fetches corresponding boxscores.
      4. Marks the date as 'completed' after processing.
    """
    logger.info(f"Starting processing for date: {dt_obj.date()}")
    # update_processing_status(db, dt_obj.date(), 'processing')

    # Format the date as MM/DD/YYYY (expected by fetch_nba_games)
    date_str = dt_obj.strftime("%m/%d/%Y")

    # Fetch NBA games for this single day
    games_data = fetch_nba_games_task(db, date_str, date_str, logger=logger, proxy=proxy)
    if games_data is None:
        logger.info(f"Error while fetching games data for {date_str}. Quit without updating status.")
        raise ValueError("failed to fetch games")
    elif games_data:  # non-empty dict
        logger.info(f"Fetched games data for {date_str}.")
    else: # empty dict
        logger.info(f"No games data fetched for {date_str}.")

    # Extract game IDs from the fetched data if available.
    # (Assuming games_data is a dict with key "LeagueGameFinderResults")
    game_ids = []
    if games_data and "LeagueGameFinderResults" in games_data:
        for game in games_data["LeagueGameFinderResults"]:
            if "GAME_ID" in game:
                game_ids.append(game["GAME_ID"])
    logger.info(f"Found {len(set(game_ids))} game(s) for {date_str}.")

    # If there are any game IDs, fetch boxscores.
    if game_ids:
        for game_id in set(game_ids):
            box_score = fetch_single_box_score(game_id=game_id, proxy=proxy)
            upsert_all_data_sets(db, box_score)
            log_api_call(db, "BoxScoreTraditionalV2", True)
            logger.info(f"Successfully fetched boxscore for game {game_id}")
        update_processing_status(db, dt_obj.date(), 'processed')
    else:
        update_processing_status(db, dt_obj.date(), 'no_games')
        logger.info("No games available to fetch boxscores for.")


    logger.info(f"Completed processing for date: {dt_obj.date()}.")


@flow(log_prints=True)
def process_pending_dates() -> None:
    """Processes all pending dates by fetching games and boxscores."""
    logger = get_run_logger()
    logger.info("process_pending_dates")
    with db_connection() as db:
        pending_dates = get_pending_dates(db)
    if not pending_dates:
        logger.info("No pending dates to process.")
        return
    logger.info(f"pending dates: {pending_dates}")
    proxy = Variable.get("proxy_list", default={}).get("proxy")
    logger.info(f"using proxies: {len(proxy) if proxy else 0}")
    with db_connection() as db:
        for date_val in pending_dates:
            dt_obj = convert_to_datetime(date_val, logger=logger)
            if dt_obj:
                process_single_date(db=db, dt_obj=dt_obj, logger=logger, proxy=proxy)


if __name__ == "__main__":
    process_pending_dates.serve(
        name="fill-pending-deployment",
        cron="25 9 * * *",  # 12:25 every day dubai time
    )
