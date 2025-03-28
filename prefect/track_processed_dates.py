#!/usr/bin/env python3
from datetime import datetime, timedelta

import dataset
from dataset import Database
from prefect import flow, get_run_logger

from helper import db_connection

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


def update_processing_status(db, date_val, status):
    """
    Updates the processing status for a given date.
    """
    logger = get_run_logger()
    table: dataset.Table = db[PROCESSING_TABLE]
    table.update(
        row={
            'date': date_val,
            'status': status,
            'updated_at': datetime.now()
        },
        keys=['date'])
    logger.info(f"Updated status for {date_val} to '{status}'.")


def get_pending_dates(db):
    """
    Retrieves a list of dates (as stored in the processing_status table) that are marked as 'pending'.
    """
    table = db[PROCESSING_TABLE]
    pending = list(table.find(status='pending'))
    dates = [row['date'] for row in pending]
    return dates


@flow(log_prints=True)
def track_dates(last_n_days: int = 3):
    """
    Main function that:
      - Connects to the database.
      - Ensures the processing_status table exists.
      - Retrieves all dates marked as 'pending'.
      - Processes each pending date sequentially.
    """
    logger = get_run_logger()
    logger.info(f"track_dates: {last_n_days}")
    start_date = datetime.today() - timedelta(days=last_n_days)
    end_date = datetime.today() - timedelta(days=1)

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
