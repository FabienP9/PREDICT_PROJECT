'''
This tests file concern all functions in the games_details_extraction_lnb.
It units test the happy path for each function
'''

from unittest.mock import MagicMock, patch
import pandas as pd
from pandas.testing import assert_frame_equal

from src.predict_core.games_details_extraction.games_details_extraction_lnb import games_details_extraction_lnb

def test_get_token():

    # this test the get_token function
    subset_number = 1
    fake_json = {"token": "abc123xyz789"}
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = fake_json
    mock_session.get.return_value = mock_response

    expected_token = fake_json["token"]

    with patch.object(games_details_extraction_lnb.requests.Session, "get", return_value = mock_response):
        result_token = games_details_extraction_lnb.get_token(subset_number, mock_session)
        assert result_token == expected_token

def test_get_games_subset():

    # this test the get_games_subset function - happy path
    subset_number = 1
    fake_payload = {"league_id": 1}
    fake_token = "abc123xyz789"
    fake_json_response = {
        "data": [
            {"data": [{"match_id": 101, "home_team": "Team A", "away_team": "Team B"}]},
            {"data": [{"match_id": 102, "home_team": "Team C", "away_team": "Team D"}]}
        ]
    }
    anchor_game_id = None
    
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = fake_json_response
    mock_session.post.return_value = mock_response
    
    expected_columns = ["match_id", "home_team", "away_team"]

    with patch.object(games_details_extraction_lnb, "get_token", return_value=fake_token):
        with patch.object(games_details_extraction_lnb.requests.Session, "post", return_value=mock_response):
            result_df = games_details_extraction_lnb.get_games_subset(
                subset_number, 
                mock_session, 
                fake_payload.copy(), 
                anchor_game_id
            )
            
            assert len(result_df) == 2
            assert list(result_df.columns) == expected_columns
            assert mock_session.post.called
            assert "Authorization" in mock_session.post.call_args[1]["headers"]

def test_extract_nested_teams_scores():

    # this test the extract_nested_team_scores function
    teams_scores = [
        {"team": "A", "score": 105},
        {"team": "B", "score": 112}
    ]
    result = games_details_extraction_lnb.extract_nested_teams_scores(teams_scores, index=0, field="score")
    assert result == 105

def test_get_game_details_lnb_with_gameday(read_csv, read_json):

    # this tests the get_game_details_lnb function with gameday
    competition_source_id = 317
    season_division_number = 1
    season_name = "2026-2027"
    gameday = '30ème journée'

    df_games = pd.json_normalize(read_json("lnb_response.json").get("data", []), record_path="data", errors="ignore")
    expected_df = read_csv("game_from_lnb_response_gameday.csv")

    # Mock get_games_subset to return data once, then empty (stops pagination loop)
    with patch.object(games_details_extraction_lnb, "get_games_subset") as mock_get_games_subset:
        # First call returns game data, subsequent calls return empty DataFrame
        mock_get_games_subset.side_effect = [
            df_games.copy(),           # Iteration 1: has data
            pd.DataFrame()             # Iteration 2+: empty (loop terminates)
        ]
        
        result_df = games_details_extraction_lnb.get_game_details_lnb(
            competition_source_id,
            season_division_number,
            season_name,
            gameday
        )
        assert_frame_equal(
            result_df[1:].astype(str).reset_index(drop=True), 
            expected_df[1:].astype(str).reset_index(drop=True), 
            check_dtype=False
        )



def test_get_game_details_lnb_without_gameday(read_csv, read_json):

    # this tests the get_game_details_lnb function with gameday
    competition_source_id = 317
    season_division_number = 1
    season_name = "2026-2027"

    df_games = pd.json_normalize(read_json("lnb_response.json").get("data", []), record_path="data", errors="ignore")
    expected_df = read_csv("game_from_lnb_response.csv")

    # Mock get_games_subset to return data once, then empty (stops pagination loop)
    with patch.object(games_details_extraction_lnb, "get_games_subset") as mock_get_games_subset:
        # First call returns game data, subsequent calls return empty DataFrame
        mock_get_games_subset.side_effect = [
            df_games.copy(),           # Iteration 1: has data
            pd.DataFrame()             # Iteration 2+: empty (loop terminates)
        ]
        
        result_df = games_details_extraction_lnb.get_game_details_lnb(
            competition_source_id,
            season_division_number,
            season_name
        )
        assert_frame_equal(
            result_df[1:].astype(str).reset_index(drop=True), 
            expected_df[1:].astype(str).reset_index(drop=True), 
            check_dtype=False
        )

