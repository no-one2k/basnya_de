from nba_api.stats.endpoints import LeagueGameFinder
from nba_api.stats.library.parameters import LeagueIDNullable




if __name__ == "__main__":
    start_date = '03/19/2025'
    end_date = '03/19/2025'
    proxy_str = """host
port
username
password
    """
    proxy_parts = [part.strip() for part in proxy_str.split('\n') if part.strip()]
    host = proxy_parts[0]
    port = proxy_parts[1]
    username = proxy_parts[2]
    password = proxy_parts[3]

    proxy_auth = f"{username}:{password}@{host}:{port}"
    proxy = [f"http://{proxy_auth}"]
    print(proxy)
    game_finder = LeagueGameFinder(
        league_id_nullable=LeagueIDNullable.nba,
        date_from_nullable=start_date,
        date_to_nullable=end_date,
        proxy=proxy,
    )
    print(game_finder)
    res = game_finder.get_normalized_dict()
    if res:
        for data_sets_key, records in res.items():
            print(data_sets_key, records)
    else:
        print('nothing')
