'''
This tests file concern all functions in the games_details_extraction_lnb.
It units test the happy path for each function
'''

from unittest.mock import MagicMock, patch
from pandas.testing import assert_frame_equal

from src.predict_core.games_details_extraction.games_details_extraction_lnb import games_details_extraction_lnb

def test_get_game_details_lnb_with_gameday_without_modification(read_csv, read_json):

    # this test the get_game_details_lnb function having a gameday which has no modifcation
    competition_row = next(read_csv("competition_unique.csv").itertuples(index=False))
    gameday = '1ere journee'
    df_gameday_modification = read_csv("gameday_modification.csv")
    fake_json = read_json("lnb_game_response.json")
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = fake_json
    expected_df = read_csv("game.csv")

    with patch.object(games_details_extraction_lnb.requests, "post", return_value = mock_lnb_response):
        result_df = games_details_extraction_lnb.get_game_details_lnb(competition_row,gameday,df_gameday_modification)
        assert_frame_equal(result_df[1:].astype(str).reset_index(drop=True), expected_df[1:].astype(str).reset_index(drop=True),check_dtype=False)

def test_get_game_details_lnb_with_gameday_with_modification(read_csv, read_json):

    # this test the get_game_details_lnb function having a gameday which has no modifcation
    competition_row = next(read_csv("competition_unique.csv").itertuples(index=False))
    gameday = '2ème journée'
    df_gameday_modification = read_csv("gameday_modification.csv")
    fake_json = read_json("lnb_game_response.json")
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = fake_json
    expected_df = read_csv("game.csv")

    with patch.object(games_details_extraction_lnb.requests, "post", return_value = mock_lnb_response):
        result_df = games_details_extraction_lnb.get_game_details_lnb(competition_row,gameday,df_gameday_modification)
        assert_frame_equal(result_df.astype(str).reset_index(drop=True), expected_df.head(1).astype(str).reset_index(drop=True),check_dtype=False)

def test_get_game_details_lnb_without_gameday(read_csv, read_json):

    # this test the get_game_details_lnb function without a gameday
    competition_row = next(read_csv("competition_unique.csv").itertuples(index=False))
    fake_json = read_json("lnb_game_response.json")
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = fake_json
    expected_df = read_csv("game.csv")

    with patch.object(games_details_extraction_lnb.requests, "post", return_value = mock_lnb_response):
        result_df = games_details_extraction_lnb.get_game_details_lnb(competition_row)
        assert_frame_equal(result_df[1:].astype(str).reset_index(drop=True), expected_df[1:].astype(str).reset_index(drop=True),check_dtype=False)
