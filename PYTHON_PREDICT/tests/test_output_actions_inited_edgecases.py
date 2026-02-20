'''
This tests file concern all functions in the output_actions_inited module.
It units test unexpected paths
'''
import unittest
from unittest.mock import patch
import pandas as pd
from datetime import datetime as dt
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import output_actions_inited
from testutils import assertExit
from testutils import read_txt
from testutils import read_json

def test_transform_inited_games_to_list_empty():
    
    # this test the function transform_inited_games_to_listwith an empty dataframe. Must result in an empty string
    df_games = pd.read_csv("materials/edgecases/qGame_empty.csv")

    result = output_actions_inited.transform_inited_games_to_list(df_games)
    assert result == ""

def test_transform_inited_games_to_list_missing_column():

    # this test the function transform_inited_games_to_list with a missing column. Must exit the program
    df_games = pd.read_csv("materials/edgecases/qGame_missing_game_message.csv")
    assertExit(lambda: output_actions_inited.transform_inited_games_to_list(df_games))

def test_transform_inited_games_to_calendar_empty():
    
    # this test the function transform_inited_games_to_calendar with an empty dataframe. Must exit the program
    
    df_games = pd.read_csv("materials/edgecases/qGame_empty.csv")
    assertExit(lambda: output_actions_inited.transform_inited_games_to_calendar(df_games))

def test_transform_inited_games_to_calendar_bad_type():
    
    # this test the function transform_inited_games_to_calendar with a string instead of time. Must exit the program, but should not happen due to test in database.
    df_games = pd.read_csv("materials/edgecases/qGame_badtype.csv")
    assertExit(lambda: output_actions_inited.transform_inited_games_to_calendar(df_games))

def test_create_inited_message_no_remaining_games():
    
    # this test the function create_inited_message with NB_GAMES_OPENED = 0. Must not change #REMAINING_GAMEDAYS#
    param_dict = {
        'GAMEDAY_OPENING': '1ere journee', 
        'NB_GAMES_OPENING': 2, 
        'CALENDAR_GAMES_OPENING': read_txt("materials/output_actions_inited_get_inited_calendar_games.txt"), 
        'FIRSTGAMETIME_OPENING': '__L__WEEKDAY_1__L__ 01/01 15h',
        'LIST_GAMES_OPENING': read_txt("materials/output_actions_inited_get_inited_list_games.txt"), 
        'NB_GAMES_OPENED': 0, 
        'LIST_GAMEDAYS_OPENED': '', 
        'CALENDAR_GAMES_OPENED': '', 
        'LIST_GAMES_OPENED': '',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP':1
    }
    
    template = read_txt("materials/output_gameday_init_template_france.txt")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_filename = "result.txt"
    expected_result = read_txt("materials/edgecases/forumoutput_inited_s1_1erejournee_france_no_gamedays.txt")

    with patch("output_actions_inited.outputA.define_filename", return_value=mock_filename) as mock_filename, \
         patch("output_actions_inited.fileA.create_txt") as mock_create_txt:
        
        content, country, forum = output_actions_inited.create_inited_message(param_dict, template, country, forum, sr_gameday_output_init)
        assert content.split() == expected_result.split()
        assert country == "FRANCE"
        assert forum == 'BI'
        
def test_create_inited_message_none_param():
    
    # this test the function create_inited_message with no parameters. Must exit the program
    param_dict = None
    template = read_txt("materials/output_gameday_init_template_france.txt")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    
    assertExit(lambda: output_actions_inited.create_inited_message(param_dict, template, country, forum, sr_gameday_output_init))

def test_process_output_message_inited_with_no_topics():
    
    # this test the function process_output_message_inited with no topics provided. multithreading_run for posting should be called with empty list
    context_dict = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'str_output_gameday_init_template_FRANCE': read_txt("materials/output_gameday_init_template_france.txt"),
        'str_output_gameday_init_template_ITALIA': read_txt("materials/output_gameday_init_template_france.txt")
    }
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_topics = pd.read_csv("materials/edgecases/qTopics_Init_empty.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}
    mock_params_derived = {'dummy':'param_derived'}
    mock_messages = [("fake_content_FRANCE", "FRANCE","BI"), ("fake_content_ITALIA", "ITALIA","II")]

    # Patch all external dependencies
    with patch("output_actions_inited.snowflake_execute", return_value=mock_df_topics), \
         patch("output_actions_inited.get_inited_parameters", return_value=mock_params_retrieved), \
         patch("output_actions_inited.config.multithreading_run", return_value=[]) as mock_mt, \
         patch("output_actions_inited.post_message"):

        # Call the function
        output_actions_inited.process_output_message_inited(context_dict, sr_gameday_output_init)
        # posting step should have been called once with []
        called_args = mock_mt.call_args_list[-1][0][1]
        assert called_args == []
        
if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_list_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_list_missing_column))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_calendar_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_calendar_bad_type))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_message_no_remaining_games))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_message_none_param))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_inited_with_no_topics))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)