from nba_api.stats.endpoints import LeagueGameFinder
from nba_api.stats.library.parameters import LeagueIDNullable

if __name__ == "__main__":
    start_date = '03/19/2025'
    end_date = '03/19/2025'
    game_finder = LeagueGameFinder(
            league_id_nullable=LeagueIDNullable.nba,
            date_from_nullable=start_date,
            date_to_nullable=end_date,
        )
    print(game_finder)
    res = game_finder.get_normalized_dict()
    if res:
        for data_sets_key, records in res.items():
            print(data_sets_key, records)
    else:
        print('nothing')
