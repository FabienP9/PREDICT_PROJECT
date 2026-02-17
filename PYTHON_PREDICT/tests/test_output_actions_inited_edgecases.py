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

def test_get_inited_list_games_empty_df():
    
    # this test the function get_inited_list_games with snowflake returning empty dataframe. Must return an empty string.
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_games = pd.read_csv("materials/edgecases/qGame_empty.csv")
    
    with patch.object(output_actions_inited, 'snowflake_execute', return_value=mock_df_games):
        list_games, nb_games = output_actions_inited.get_inited_list_games(sr_snowflake_account, sr_gameday_output_init)
    
        assert list_games == ""
        assert nb_games == 0

def test_get_inited_list_games_special_chars():

    # this test the function get_inited_list_games with special characters in dataframe. Must be accepted
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_games = pd.read_csv("materials/edgecases/qGame_specialchars.csv")
    expected_str = read_txt("materials/edgecases/output_actions_inited_get_inited_list_games_specialchars.txt")

    with patch.object(output_actions_inited, 'snowflake_execute', return_value=mock_df_games):
        list_games, nb_games = output_actions_inited.get_inited_list_games(sr_snowflake_account, sr_gameday_output_init)   
        assert list_games.split() == expected_str.split()
        assert nb_games == 2

def test_get_inited_list_games_missing_key():
    
    # this test the function get_inited_list_games with missing SEASON_ID and missing GAMEDAY. Must exit program
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/edgecases/sr_gameday_output_calculate_noseasonid.csv").iloc[0]
    mock_df_games = pd.read_csv("materials/qGame.csv")
    
    with patch.object(output_actions_inited, 'snowflake_execute', return_value=mock_df_games):
        assertExit(lambda: output_actions_inited.get_inited_list_games(sr_snowflake_account, sr_gameday_output_init))

def test_get_inited_remaining_games_duplicate_gamedays():
    
    # this test the function get_inited_remaining_games with duplicates gamedays. Must accept it
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_remaining = pd.read_csv("materials/edgecases/qGame_Remaining_AtDate_twice_gameday.csv")
    expected_str_games = read_txt("materials/edgecases/output_actions_inited_get_inited_remaining_games_twice_gameday.txt")

    with patch.object(output_actions_inited, 'snowflake_execute', return_value=mock_df_remaining):
        rem_days, rem_games, nb_rem = output_actions_inited.get_inited_remaining_games(sr_snowflake_account, sr_gameday_output_init)
        
        assert nb_rem == 3
        assert rem_days == '3eme journee , 4eme journee'
        assert rem_games.split() == expected_str_games.split()

def test_get_inited_remaining_games_empty_df():
    
    # this test the function get_inited_remaining_games with empty dataframe. Must return eempty string
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_remaining = pd.read_csv("materials/edgecases/qGame_Remaining_AtDate_empty.csv")

    with patch.object(output_actions_inited, 'snowflake_execute', return_value=mock_df_remaining):
        rem_days, rem_games, nb_rem = output_actions_inited.get_inited_remaining_games(sr_snowflake_account, sr_gameday_output_init)
        
        assert nb_rem == 0
        assert rem_days == ''
        assert rem_games == ''

def test_get_inited_dategame1_midnight_time():
    
    # this test the function get_inited_dategame1 with midnight time. Minute 00 must not be displayed
    sr_gameday_output_init = pd.read_csv("materials/edgecases/sr_gameday_output_init_midnight.csv").iloc[0]
    sr_gameday_output_init['BEGIN_DATE_LOCAL'] = dt.strptime(sr_gameday_output_init['BEGIN_DATE_LOCAL'], "%Y-%m-%d")
    dategame1 = output_actions_inited.get_inited_dategame1(sr_gameday_output_init)
    assert dategame1 == "__L__WEEKDAY_1__L__ 01/01 0h"

def test_get_inited_dategame1_invalid_time():
    
    # this test the function get_inited_dategame1 with invalid time. Must exit the program
    sr_gameday_output_init = pd.read_csv("materials/edgecases/sr_gameday_output_init_invalid_time.csv").iloc[0]
    sr_gameday_output_init['BEGIN_DATE_LOCAL'] = dt.strptime(sr_gameday_output_init['BEGIN_DATE_LOCAL'], "%Y-%m-%d")
    assertExit(lambda: output_actions_inited.get_inited_dategame1(sr_gameday_output_init))

def test_create_inited_message_no_remaining_games():
    
    # this test the function create_inited_message with NB_GAMES_REMAINING = 0. Must not change #REMAINING_GAMEDAYS#
    param_dict = {
        'DATEGAME1': "__L__WEEKDAY_1__L__ 01/01 20h",
        'GAMEDAY': '1ere journee',
        'LIST_GAMES': read_txt("materials/output_actions_inited_get_inited_list_games.txt"),
        'BONUS_GAME': '#1EJ.BN# __L__Bonus game ID__L__ ==> __F__italicbegin__F__1EJ.01__F__italicend__F__',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'NB_GAMES_REMAINING': 0,
        'REMAINING_GAMEDAYS': '',
        'REMAINING_GAMES_FRANCE': ""
    }

    template = read_txt("materials/output_actions_gameday_init_template_france.txt")
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
    template = read_txt("materials/output_actions_gameday_init_template_france.txt")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    
    assertExit(lambda: output_actions_inited.create_inited_message(param_dict, template, country, forum, sr_gameday_output_init))

def test_process_output_message_inited_with_no_topics():
    
    # this test the function process_output_message_inited with no topics provided. multithreading_run for posting should be called with empty list
    context_dict = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'str_output_gameday_init_template_FRANCE': read_txt("materials/output_actions_gameday_init_template_france.txt"),
        'str_output_gameday_init_template_ITALIA': read_txt("materials/output_actions_gameday_init_template_france.txt")
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
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_list_games_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_list_games_special_chars))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_list_games_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_remaining_games_duplicate_gamedays))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_remaining_games_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_dategame1_midnight_time))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_dategame1_invalid_time))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_message_no_remaining_games))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_message_none_param))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_inited_with_no_topics))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)