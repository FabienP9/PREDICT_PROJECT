'''
This tests file concern all functions in the output_actions_inited module.
It units test the happy path for each function
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
from testutils import read_txt
from testutils import read_json

def test_get_inited_list_games():
    
    # this test the function get_inited_list_games
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_games = pd.read_csv("materials/qGame.csv")
    expected_str = read_txt("materials/output_actions_inited_get_inited_list_games.txt")

    with patch.object(output_actions_inited, 'snowflake_execute', return_value=mock_df_games):
        list_games, nb_games = output_actions_inited.get_inited_list_games(sr_snowflake_account, sr_gameday_output_init)
    
        assert list_games == expected_str
        assert nb_games == 2

def test_get_inited_remaining_games():
    
    # this test the function get_inited_remaining_games
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_remaining = pd.read_csv("materials/qGame_Remaining_AtDate.csv")
    expected_str_games = read_txt("materials/output_actions_inited_get_inited_remaining_games.txt")

    with patch.object(output_actions_inited, 'snowflake_execute', return_value=mock_df_remaining):
        rem_days, rem_games, nb_rem = output_actions_inited.get_inited_remaining_games(sr_snowflake_account, sr_gameday_output_init)
        
        assert nb_rem == 2
        assert rem_days == '3eme journee , 4eme journee'
        assert rem_games == expected_str_games

def test_get_inited_dategame1():
    
    # this test the function get_inited_dategame1
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    sr_gameday_output_init['BEGIN_DATE_LOCAL'] = dt.strptime(sr_gameday_output_init['BEGIN_DATE_LOCAL'], "%Y-%m-%d")
    dategame1 = output_actions_inited.get_inited_dategame1(sr_gameday_output_init)
    assert dategame1 == "__L__WEEKDAY_1__L__ 01/01 20h"

def test_get_inited_parameters():
    
    # this test the function get_inited_parameters
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_str_list_games = read_txt("materials/output_actions_inited_get_inited_list_games.txt")
    mock_str_list_remaining_games = read_txt("materials/output_actions_inited_get_inited_remaining_games.txt")
    mock_remaining_days = '3eme journee , 4eme journee'
    mock_str_date_game1 = "__L__WEEKDAY_1__L__ 01/01 20h"

    with patch('output_actions_inited.get_inited_list_games', return_value=(mock_str_list_games, 2)), \
         patch('output_actions_inited.get_inited_remaining_games', return_value=(mock_remaining_days, mock_str_list_remaining_games, 2)), \
         patch('output_actions_inited.get_inited_dategame1', return_value=mock_str_date_game1):

        output_actions_inited.get_inited_parameters(sr_snowflake_account, sr_gameday_output_init)
 
def test_create_inited_message():
    
    # this test the function create_inited_message
    param_dict = {
        'DATEGAME1': "__L__WEEKDAY_1__L__ 01/01 20h",
        'GAMEDAY': '1ere journee',
        'LIST_GAMES': read_txt("materials/output_actions_inited_get_inited_list_games.txt"),
        'BONUS_GAME': '#1EJ.BN# __L__Bonus game ID__L__ ==> __F__italicbegin__F__1EJ.01__F__italicend__F__',
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1,
        'NB_GAMES_REMAINING': 2,
        'REMAINING_GAMEDAYS': '3eme journee , 4eme journee',
        'REMAINING_GAMES': read_txt("materials/output_actions_inited_get_inited_remaining_games.txt")
    }
    template = read_txt("materials/output_actions_gameday_init_template_france.txt")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_filename = "result.txt"
    expected_result = read_txt("materials/forumoutput_inited_s1_1erejournee_france_bi.txt")

    with patch("output_actions_inited.outputA.define_filename", return_value=mock_filename) as mock_filename, \
         patch("output_actions_inited.fileA.create_txt") as mock_create_txt:
        
        content, country, forum = output_actions_inited.create_inited_message(param_dict, template, country, forum, sr_gameday_output_init)
        assert content.split() == expected_result.split()
        assert country == "FRANCE"
        assert forum == 'BI'

def test_process_output_message_inited():
    
    # this test the function process_output_message_inited
    context_dict = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'str_output_gameday_init_template_FRANCE': read_txt("materials/output_actions_gameday_init_template_france.txt"),
        'str_output_gameday_init_template_ITALIA': read_txt("materials/output_actions_gameday_init_template_france.txt")
    }
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_topics = pd.read_csv("materials/qTopics_Init.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}
    mock_messages = [("fake_content_FRANCE", "FRANCE", "BI"), ("fake_content_ITALIA", "ITALIA", "II")]

    with patch("output_actions_inited.snowflake_execute", return_value=mock_df_topics), \
         patch("output_actions_inited.get_inited_parameters", return_value=mock_params_retrieved), \
         patch("output_actions_inited.config.multithreading_run", return_value=mock_messages), \
         patch("output_actions_inited.post_message"):

        output_actions_inited.process_output_message_inited(context_dict, sr_gameday_output_init)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_list_games))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_remaining_games))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_dategame1))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_parameters))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_message))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_inited))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
