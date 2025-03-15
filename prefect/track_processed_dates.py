#!/usr/bin/env python3
import logging
import sys
from datetime import datetime, timedelta

import dataset
from dataset import Database
from prefect import flow

from helper import db_connection, convert_to_datetime
from get_games import fetch_nba_games

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Name of the table tracking processing status
PROCESSING_TABLE = "date_processing_status"


def populate_pending_dates(db: Database, start: datetime, end: datetime):
    """
    Populate processing status table with dates between start and end as 'pending'.

    Args:
        db: Dataset database connection
        start: Start datetime
        end: End datetime
    """
    table = db[PROCESSING_TABLE]
    current = start

    while current <= end:
        table.insert_ignore(
            row={
                'date': current.date(),
                'status': 'pending',
                'created_at': datetime.now()
            },
            keys=['date'])
        current += timedelta(days=1)

def get_pending_dates(db):
    """
    Retrieves a list of dates (as stored in the processing_status table) that are marked as 'pending'.
    """
    table = db[PROCESSING_TABLE]
    pending = list(table.find(status='pending'))
    dates = [row['date'] for row in pending]
    return dates


def update_processing_status(db, date_val, status):
    """
    Updates the processing status for a given date.
    """
    table: dataset.Table = db[PROCESSING_TABLE]
    table.update(
        row={
            'date': date_val,
            'status': status,
            'updated_at': datetime.now()
        },
        keys=['date'])
    logger.info(f"Updated status for {date_val} to '{status}'.")


def process_single_date(db, dt_obj):
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
    games_data = fetch_nba_games(db, date_str, date_str)
    if games_data:
        logger.info(f"Fetched games data for {date_str}.")
    else:
        logger.info(f"No games data fetched for {date_str}.")

    # Extract game IDs from the fetched data if available.
    # (Assuming games_data is a dict with key "LeagueGameFinderResults")
    game_ids = []
    if games_data and "LeagueGameFinderResults" in games_data:
        for game in games_data["LeagueGameFinderResults"]:
            if "GAME_ID" in game:
                game_ids.append(game["GAME_ID"])
    logger.info(f"Found {len(game_ids)} game(s) for {date_str}.")

    # If there are any game IDs, fetch boxscores.
    if game_ids:
        # Import fetch_boxscores from get_boxscores.py
        from get_boxscores import fetch_boxscores
        fetch_boxscores(db, game_ids)
        update_processing_status(db, dt_obj.date(), 'processed')
    else:
        update_processing_status(db, dt_obj.date(), 'no_games')
        logger.info("No games available to fetch boxscores for.")


    logger.info(f"Completed processing for date: {dt_obj.date()}.")

def process_pending_dates() -> None:
    """Processes all pending dates by fetching games and boxscores."""
    with db_connection() as db:
        pending_dates = get_pending_dates(db)
        if not pending_dates:
            logger.info("No pending dates to process.")
            return

        for date_val in pending_dates:
            dt_obj = convert_to_datetime(date_val)
            if dt_obj:
                process_single_date(db, dt_obj)

@flow(log_prints=True)
def track_dates(last_n_days: int = 3):
    """
    Main function that:
      - Connects to the database.
      - Ensures the processing_status table exists.
      - Retrieves all dates marked as 'pending'.
      - Processes each pending date sequentially.
    """
    print(f"track_dates: {last_n_days}")
    start_date = datetime.today() - timedelta(days=last_n_days)
    end_date = datetime.today()

    with db_connection() as db:
        populate_pending_dates(db, start=start_date, end=end_date)
        pending_dates = get_pending_dates(db)
        logger.info(f"Populated dates from {start_date.date()} to {end_date.date()}. "
                    f"Total pending dates: {len(pending_dates)}")

if __name__ == "__main__":
    track_dates.serve(
        name="track-dates-deployment",
        parameters={"last_n_days": 3},
        # interval=4*60*60,
        cron="25 8 * * *",  # 12:25 every day dubai time
    )
