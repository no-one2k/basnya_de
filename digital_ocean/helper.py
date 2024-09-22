from datetime import datetime

from dataset import Database, Table
from nba_api.stats.endpoints._base import Endpoint


KEY_SUSPECTS = ["SEASON_ID", "TEAM_ID", "GAME_ID", "PLAYER_ID"]

def log_api_call(db: Database, endpoint_name: str, success: bool, error_message: str = None):
    """Logs an API call to the database."""
    api_call_logs_table = db["api_call_logs"]
    api_call_logs_table.insert({
        "endpoint": endpoint_name,
        "call_time": datetime.utcnow(),
        "success": success,
        "error_message": error_message
    })


def make_raw_data_table_name(endpoint: Endpoint, data_sets_key: str) -> str:
    return f"{endpoint.__class__.__name__.lower()}__{data_sets_key.lower()}"


def upsert_all_data_sets(db: Database, endpoint: Endpoint, **upsert_kwargs):
    update_datetime = datetime.utcnow()
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
