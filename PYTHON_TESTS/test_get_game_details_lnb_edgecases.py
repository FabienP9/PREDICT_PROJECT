'''
This tests file concern all functions in the get_game_details_lnb.
It units test the enexpected path for each function, which return exception
'''
import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from game_actions.game_actions_lnb import get_game_details_lnb as get_game_details_lnb
from testutils import read_json
from testutils import assertExit

def test_get_game_details_lnb_network_failure():

    # this test the function get_game_details_lnb with network failure. Must exit the program.
    competition_row = next(pd.read_csv("materials/competition_unique.csv").itertuples(index=False))
    gameday = '1ere journee'
    fake_json = read_json("materials/lnb_game_response.json")
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = fake_json

    with patch.object(get_game_details_lnb.requests,"post", side_effect = Exception("Network error")):
        assertExit(lambda: get_game_details_lnb.get_game_details_lnb(competition_row,gameday))

def test_get_game_details_lnb_invalid_json_response():

    # this test the get_game_details_lnb function with a bad json response. Must exit the program
    competition_row = next(pd.read_csv("materials/competition_unique.csv").itertuples(index=False))
    fake_json = read_json("materials/lnb_game_response.json")
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = fake_json

    with patch.object(get_game_details_lnb.requests, "post", side_effect = ValueError("Invalid JSON")):
        assertExit(lambda: get_game_details_lnb.get_game_details_lnb(competition_row))

def test_missing_data_key():
    
    # this test the get_game_details_lnb function with a JSON without required key. Must exit the program
    competition_row = next(pd.read_csv("materials/competition_unique.csv").itertuples(index=False))
    gameday = '1ere journee'
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = {"wrong_key": []}
    
    with patch.object(get_game_details_lnb.requests,"post", return_value = mock_lnb_response):
        assertExit(lambda: get_game_details_lnb.get_game_details_lnb(competition_row,gameday))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_game_details_lnb_network_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_get_game_details_lnb_invalid_json_response))
    test_suite.addTest(unittest.FunctionTestCase(test_missing_data_key))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
