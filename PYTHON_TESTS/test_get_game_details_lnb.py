'''
This tests file concern all functions in the get_game_details_lnb.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from game_actions.game_actions_lnb import get_game_details_lnb as get_game_details_lnb
from testutils import read_json

def test_get_game_details_lnb_with_gameday():

    # this test the get_game_details_lnb function having a gameday
    competition_row = next(pd.read_csv("materials/competition_unique.csv").itertuples(index=False))
    gameday = '1ere journee'
    fake_json = read_json("materials/lnb_game_response.json")
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = fake_json
    expected_df = pd.read_csv("materials/game.csv")

    with patch.object(get_game_details_lnb.requests, "post", return_value = mock_lnb_response):
        result_df = get_game_details_lnb.get_game_details_lnb(competition_row,gameday)
        assert_frame_equal(result_df[1:].astype(str).reset_index(drop=True), expected_df[1:].astype(str).reset_index(drop=True),check_dtype=False)

def test_get_game_details_lnb_without_gameday():

    # this test the get_game_details_lnb function without a gameday
    competition_row = next(pd.read_csv("materials/competition_unique.csv").itertuples(index=False))
    fake_json = read_json("materials/lnb_game_response.json")
    mock_lnb_response = MagicMock()
    mock_lnb_response.json.return_value = fake_json
    expected_df = pd.read_csv("materials/game.csv")

    with patch.object(get_game_details_lnb.requests, "post", return_value = mock_lnb_response):
        result_df = get_game_details_lnb.get_game_details_lnb(competition_row)
        assert_frame_equal(result_df[1:].astype(str).reset_index(drop=True), expected_df[1:].astype(str).reset_index(drop=True),check_dtype=False)
        
if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_game_details_lnb_with_gameday))
    test_suite.addTest(unittest.FunctionTestCase(test_get_game_details_lnb_without_gameday))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
