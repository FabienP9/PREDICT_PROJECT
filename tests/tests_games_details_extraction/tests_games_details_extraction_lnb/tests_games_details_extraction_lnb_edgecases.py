'''
This tests file concern all functions in the games_details_extraction_lnb.
It units test the enexpected path for each function, which return exception
'''
from unittest.mock import MagicMock, patch
import pandas as pd

from src.predict_core.games_details_extraction.games_details_extraction_lnb import games_details_extraction_lnb

def test_get_token_bad_status_code(assert_exit):

    # this test the get_token function when LNB API returns non-200 status. Must exit the program
    subset_number = 1
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    
    with patch.object(games_details_extraction_lnb.requests.Session, "get", return_value = mock_response):
        assert_exit(lambda: games_details_extraction_lnb.get_token(subset_number, mock_session))

def test_get_token_missing_token_in_response(assert_exit):

    # this test the get_token function when the response JSON contains no token key. Must exit the program
    subset_number = 1
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"error": "no token found"}
    mock_session = MagicMock()
    mock_session.get.return_value = mock_response
    
    with patch.object(games_details_extraction_lnb.requests.Session, "get", return_value = mock_response):
        assert_exit(lambda: games_details_extraction_lnb.get_token(subset_number, mock_session))

def test_get_games_subset_non_200_status(assert_exit):

    # this tests the get_games_subset function with non-200 status code
    subset_number = 1
    fake_payload = {"league_id": 1}
    fake_token = "abc123xyz789"
    fake_error_text = "<html>Internal Server Error</html>"[:500]
    
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = fake_error_text
    mock_session.post.return_value = mock_response

    
    with patch.object(games_details_extraction_lnb,'get_token', return_value=fake_token), \
        patch.object(games_details_extraction_lnb.requests.Session, "post", return_value=mock_response):
        
        assert_exit(lambda: games_details_extraction_lnb.get_games_subset(
                    subset_number, 
                    mock_session, 
                    fake_payload.copy(), 
                    None
                ))

def test_get_games_subset_with_anchor_game_id():

    # this tests the get_games_subset function - anchor_game_id modifies payload incorrectly
    subset_number = 1
    fake_payload = {"league_id": 1}
    fake_token = "abc123xyz789"
    fake_json_response = {
        "data": [
            {"data": [{"match_id": 101, "home_team": "Team A"}]}
        ]
    }
    anchor_game_id = 555
    
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = fake_json_response
    mock_session.post.return_value = mock_response
    
    expected_anchor_key = "anchor_match_external_id"

    with patch.object(games_details_extraction_lnb,'get_token', return_value=fake_token):
        with patch.object(games_details_extraction_lnb.requests.Session, "post", return_value=mock_response):
            games_details_extraction_lnb.get_games_subset(
                subset_number, 
                mock_session, 
                fake_payload.copy(), 
                anchor_game_id
            )
            
            call_args = mock_session.post.call_args
            sent_payload = call_args[1]["json"]
            assert expected_anchor_key in sent_payload
            assert sent_payload[expected_anchor_key] == anchor_game_id

def test_get_game_details_lnb_with_gameday_running_game(read_json, assert_exit):

    # this tests the get_game_details_lnb function with gameday and a task to run CALCULATE_PARTIAL, with a running game
    competition_source_id = 317
    season_division_number = 1
    season_name = "2026-2027"
    gameday = '30ème journée'
    task_run = 'CALCULATE'

    df_games = pd.json_normalize(read_json("lnb_response_running.json").get("data", []), record_path="data", errors="ignore")
    

    # Mock get_games_subset to return data once, then empty (stops pagination loop)
    with patch.object(games_details_extraction_lnb, "get_games_subset") as mock_get_games_subset:
        # First call returns game data, subsequent calls return empty DataFrame
        mock_get_games_subset.side_effect = [
            df_games.copy(),           # Iteration 1: has data
            pd.DataFrame()             # Iteration 2+: empty (loop terminates)
        ]
        
        assert_exit(lambda: games_details_extraction_lnb.get_game_details_lnb(
            competition_source_id = competition_source_id,
            season_division_number = season_division_number,
            season_name = season_name,
            gameday = gameday,
            task_run = task_run
        ))