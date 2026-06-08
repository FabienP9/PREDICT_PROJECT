'''
This tests file concern all functions in the games_details_extraction_lnb.
It units test the enexpected path for each function, which return exception
'''
from unittest.mock import MagicMock, patch

from src.predict_core.games_details_extraction.games_details_extraction_lnb import games_details_extraction_lnb

def test_get_game_details_lnb_network_failure(read_csv,read_json, assert_exit):

    # this test the function get_game_details_lnb with network failure. Must exit the program.
    competition_source_id = 288
    gameday = '1ere journee'
    sr_games_to_extract = read_csv("gameday_modification.csv")['GAME_SOURCE_ID']
    fake_json = read_json("lnb_game_response.json")
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = fake_json

    with patch.object(games_details_extraction_lnb.requests,"post", side_effect = Exception("Network error")):
        assert_exit(lambda: games_details_extraction_lnb.get_game_details_lnb(competition_source_id,gameday,sr_games_to_extract))

def test_get_game_details_lnb_invalid_json_response(read_csv, read_json, assert_exit):

    # this test the get_game_details_lnb function with a bad json response. Must exit the program
    competition_source_id = 288
    fake_json = read_json("lnb_game_response.json")
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = fake_json

    with patch.object(games_details_extraction_lnb.requests, "post", side_effect = ValueError("Invalid JSON")):
        assert_exit(lambda: games_details_extraction_lnb.get_game_details_lnb(competition_source_id))

def test_missing_data_key(read_csv, assert_exit):
    
    # this test the get_game_details_lnb function with a JSON without required key. Must exit the program
    competition_source_id = 288
    gameday = '1ere journee'
    sr_games_to_extract = read_csv("gameday_modification.csv")['GAME_SOURCE_ID']
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = {"wrong_key": []}
    
    with patch.object(games_details_extraction_lnb.requests,"post", return_value = mock_lnb_response):
        assert_exit(lambda: games_details_extraction_lnb.get_game_details_lnb(competition_source_id,gameday,sr_games_to_extract))

