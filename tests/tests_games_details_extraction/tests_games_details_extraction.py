'''
This tests file concern all functions in the games_details_extraction.
It units test the happy path for each function
'''

from unittest.mock import patch
from pandas.testing import assert_frame_equal
import pandas as pd

from src.predict_core.games_details_extraction import games_details_extraction

def test_extract_games_from_competition(read_csv):

    # this test the function extract_games_from_competition
    df_competition = read_csv("competition_unique.csv")
    mock_df_game = read_csv("game.csv")
    with patch.dict(games_details_extraction.game_info_functions, {"LNB": lambda row: mock_df_game}), \
         patch.object(games_details_extraction, "create_csv"):

        result = games_details_extraction.extract_games_from_competition(df_competition)
        assert_frame_equal(result.reset_index(drop=True), mock_df_game.reset_index(drop=True),check_dtype=False)

def test_extract_games_from_need(read_csv):
    
    # this test the function extract_games_from_need
    sr_output_need = read_csv("output_need_calculate.csv").iloc[0]
    df_competition = read_csv("competition_unique.csv")
    mock_df_game = read_csv("game.csv")
    df_gameday_modification = pd.DataFrame()

    with patch.dict(games_details_extraction.game_info_functions, {"LNB": lambda *args, **kwargs: mock_df_game}), \
        patch.object(games_details_extraction, "create_csv"):

        result = games_details_extraction.extract_games_from_need(sr_output_need,df_competition,df_gameday_modification)
        assert_frame_equal(result.reset_index(drop=True), mock_df_game.reset_index(drop=True),check_dtype=False)