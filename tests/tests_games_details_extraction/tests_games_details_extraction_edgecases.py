'''
This tests file concern all functions in the games_details_extraction module.
It units test the unexpected path for each function, which return exception
'''

from unittest.mock import patch
from pandas.testing import assert_frame_equal
import pandas as pd

from src.predict_core.games_details_extraction import games_details_extraction

def test_extract_games_from_competition_empty_df(read_csv):
    
    # this test the function extract_games_from_competition with an empty df_competition. Must return an empty result
    df_competition_empty = read_csv("edgecases/competition_empty.csv")
    mock_df_game = read_csv("edgecases/game_empty.csv")
    with patch.dict(games_details_extraction.game_info_functions, {"LNB": lambda row: mock_df_game}), \
         patch.object(games_details_extraction, "create_csv"):

        result = games_details_extraction.extract_games_from_competition(df_competition_empty)
        assert_frame_equal(result.reset_index(drop=True), mock_df_game.reset_index(drop=True),check_dtype=False)

def test_extract_games_from_competition_unknown_source(read_csv, assert_exit):
    
    # this test the function extract_games_from_competition when competition source is not in game_info_functions. Must exit the program.
    df_competition = read_csv("edgecases/competition_unknown_source.csv")
    mock_df_game = read_csv("game.csv")
    with patch.dict(games_details_extraction.game_info_functions, {"LNB": lambda row: mock_df_game}), \
         patch.object(games_details_extraction, "create_csv"):

        assert_exit(lambda: games_details_extraction.extract_games_from_competition(df_competition))

def test_extract_games_from_need_no_matching_competition(read_csv, assert_exit):
    
    # this test the function extract_games_from_need when no competition match need. Must exit the program.
    sr_output_need = read_csv("output_need_calculate.csv").iloc[0]
    df_competition = read_csv("edgecases/competition_empty.csv")
    df_gameday_modification = pd.DataFrame()
    mock_df_game = read_csv("game.csv")

    with patch.dict(games_details_extraction.game_info_functions, {"LNB": lambda *args, **kwargs: mock_df_game}), \
        patch.object(games_details_extraction, "create_csv"):

        assert_exit(lambda: games_details_extraction.extract_games_from_need(sr_output_need,df_competition,df_gameday_modification))

def test_extract_games_from_need_function_raises(read_csv, assert_exit):
    
    # this test the function extract_games_from_need when no competition match needgame detail function raises an exception for a valid need. Must exit the program.
    sr_output_need = read_csv("output_need_calculate.csv").iloc[0]
    df_competition = read_csv("competition_unique.csv")
    df_gameday_modification = pd.DataFrame()

    def raise_error(_, __):
        raise ValueError("Boom!")

    with patch.dict(games_details_extraction.game_info_functions, {'LNB': raise_error}), \
        patch.object(games_details_extraction, "create_csv"):

        assert_exit(lambda: games_details_extraction.extract_games_from_need(sr_output_need,df_competition,df_gameday_modification))
