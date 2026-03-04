'''
This tests file concern all functions in the game_actions module.
It units test the unexpected path for each function, which return exception
'''

import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from game_actions import game_actions
from testutils import assertExit

def test_extract_games_from_competition_empty_df():
    
    # this test the function extract_games_from_competition with an empty df_competition. Must return an empty result
    df_competition_empty = pd.read_csv("materials/edgecases/competition_empty.csv")
    mock_df_game = pd.read_csv("materials/edgecases/game_empty.csv")
    with patch.dict(game_actions.game_info_functions, {"LNB": lambda row: mock_df_game}), \
         patch.object(game_actions, "create_csv"), \
         patch.object(game_actions, "config"):

        result = game_actions.extract_games_from_competition(df_competition_empty)
        assert_frame_equal(result.reset_index(drop=True), mock_df_game.reset_index(drop=True),check_dtype=False)

def test_extract_games_from_competition_unknown_source():
    
    # this test the function extract_games_from_competition when competition source is not in game_info_functions. Must exit the program.
    df_competition = pd.read_csv("materials/edgecases/competition_unknown_source.csv")
    mock_df_game = pd.read_csv("materials/game.csv")
    with patch.dict(game_actions.game_info_functions, {"LNB": lambda row: mock_df_game}), \
         patch.object(game_actions, "create_csv"), \
         patch.object(game_actions, "config"):

        assertExit(lambda: game_actions.extract_games_from_competition(df_competition))

def test_extract_games_from_need_no_matching_competition():
    
    # this test the function extract_games_from_need when no competition match need. Must exit the program.
    sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]
    df_competition = pd.read_csv("materials/edgecases/competition_empty.csv")
    mock_df_game = pd.read_csv("materials/game.csv")

    with patch.dict(game_actions.game_info_functions, {"LNB": lambda *args, **kwargs: mock_df_game}), \
        patch.object(game_actions, "create_csv"):

        assertExit(lambda: game_actions.extract_games_from_need(sr_output_need,df_competition))

def test_extract_games_from_need_function_raises():
    
    # this test the function extract_games_from_need when no competition match needgame detail function raises an exception for a valid need. Must exit the program.
    sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]
    df_competition = pd.read_csv("materials/competition_unique.csv")

    def raise_error(_, __):
        raise ValueError("Boom!")

    with patch.dict(game_actions.game_info_functions, {'LNB': raise_error}), \
        patch.object(game_actions, "create_csv"):

        assertExit(lambda: game_actions.extract_games_from_need(sr_output_need,df_competition))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_extract_games_from_competition_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_games_from_competition_unknown_source))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_games_from_need_no_matching_competition))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_games_from_need_function_raises))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
