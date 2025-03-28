import logging
import os
import sys
from datetime import datetime, date
from datetime import timezone
from logging import Logger
from typing import Set, Generator

import dataset
from dataset import Database, Table
from contextlib import contextmanager
from nba_api.stats.endpoints._base import Endpoint
from prefect.variables import Variable


KEY_SUSPECTS = ["SEASON_ID", "TEAM_ID", "GAME_ID", "PLAYER_ID"]


SSH_CONFIG = {
}


@contextmanager
def db_connection() -> Generator[Database, None, None]:
    db_config_source = Variable.get("db_connection", default=os.environ)
    db_config = {
        "username": db_config_source.get("DB_USER"),
        "password": db_config_source.get("DB_PASSWORD"),
        "host": db_config_source.get("DB_HOST"),
        "port": db_config_source.get("DB_PORT"),
        "database": db_config_source.get("DB_NAME"),
    }
    safe_config = {
        "host": db_config["host"],
        "port": db_config["port"],
        "database": db_config["database"]
    }
    print(safe_config)
    """Context manager for database connection via SSH tunnel."""
    if SSH_CONFIG:
        from sshtunnel import SSHTunnelForwarder
        with SSHTunnelForwarder(
                (SSH_CONFIG['host'], SSH_CONFIG['port']),
                ssh_username=SSH_CONFIG['user'],
                ssh_password=SSH_CONFIG['password'],
                remote_bind_address=(db_config['host'], db_config['port'])
        ) as tunnel:
            db_url = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{tunnel.local_bind_port}/{db_config['database']}"
            db = dataset.connect(db_url)
            try:
                yield db
            finally:
                db.close()
    else:
        db_url = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        db = dataset.connect(db_url)
        try:
            yield db
        finally:
            db.close()


def log_api_call(db: Database, endpoint_name: str, success: bool, error_message: str = None):
    """Logs an API call to the database."""
    api_call_logs_table = db["api_call_logs"]
    api_call_logs_table.insert({
        "endpoint": endpoint_name,
        "call_time": datetime.now(timezone.utc),
        "success": success,
        "error_message": error_message
    })


def make_raw_data_table_name(endpoint: Endpoint, data_sets_key: str) -> str:
    return f"{endpoint.__class__.__name__.lower()}__{data_sets_key.lower()}"


def upsert_all_data_sets(db: Database, endpoint: Endpoint, **upsert_kwargs):
    update_datetime = datetime.now(timezone.utc)
    for data_sets_key, records in endpoint.get_normalized_dict().items():
        keys = []
        for i, record in enumerate(records):
            record['updated_at'] = update_datetime
            if i == 0:
                for suspect in KEY_SUSPECTS:
                    if suspect in record:
                        keys.append(suspect)
        if records:
            _table: Table = db[make_raw_data_table_name(endpoint, data_sets_key)]
            if 'keys' not in upsert_kwargs:
                upsert_kwargs['keys'] = keys
            _table.upsert_many(records, **upsert_kwargs)


def get_distinct_game_ids(db, table_name: str, logger) -> Set[str]:
    try:
        return set(r['GAME_ID'] for r in db[table_name].distinct('GAME_ID'))
    except Exception as e:
        logger.exception(f"Table '{table_name}' not found or error accessing it: {e}")
        return set()

def convert_to_datetime(date_val, logger) -> datetime | None:
    """Converts various date formats to datetime object."""
    if isinstance(date_val, str):
        try:
            return datetime.strptime(date_val, "%Y-%m-%d")
        except Exception as e:
            logger.error(f"Invalid date format for {date_val}: {e}")
            return None
    elif isinstance(date_val, datetime):
        return date_val
    elif isinstance(date_val, date):
        return datetime.combine(date_val, datetime.min.time())
    else:
        logger.error(f"Unexpected type for date {date_val}")
        return None


def configure_logger(module_name, level=logging.INFO) -> Logger:
    # Configure logging
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(module_name)
