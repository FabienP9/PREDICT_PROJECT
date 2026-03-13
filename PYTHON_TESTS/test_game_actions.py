'''
This tests file concern all functions in the game_actions.
It units test the happy path for each function
'''

import unittest
import pandas as pd
from unittest.mock import patch
from pandas.testing import assert_frame_equal
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from game_actions import game_actions

def test_extract_games_from_competition():

    # this test the function extract_games_from_competition
    df_competition = pd.read_csv("materials/competition_unique.csv")
    mock_df_game = pd.read_csv("materials/game.csv")
    with patch.dict(game_actions.game_info_functions, {"LNB": lambda row: mock_df_game}), \
         patch.object(game_actions, "create_csv"), \
         patch.object(game_actions, "config"):

        result = game_actions.extract_games_from_competition(df_competition)
        assert_frame_equal(result.reset_index(drop=True), mock_df_game.reset_index(drop=True),check_dtype=False)

def test_extract_games_from_need():
    
    # this test the function extract_games_from_need
    sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]
    df_competition = pd.read_csv("materials/competition_unique.csv")
    mock_df_game = pd.read_csv("materials/game.csv")

    with patch.dict(game_actions.game_info_functions, {"LNB": lambda *args, **kwargs: mock_df_game}), \
        patch.object(game_actions, "create_csv"):

        result = game_actions.extract_games_from_need(sr_output_need,df_competition)
        assert_frame_equal(result.reset_index(drop=True), mock_df_game.reset_index(drop=True),check_dtype=False)


if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_extract_games_from_competition))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_games_from_need))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
