'''
This tests file concern all functions in the output_actions_inited module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
import pandas as pd
import numpy as np
from datetime import datetime as dt
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import output_actions_inited
from testutils import read_txt
from testutils import read_json

def test_transform_inited_games_to_list():

    # this test the function transform_inited_games_to_list
    df_games = pd.read_csv("materials/qGame.csv")
    expected_str = read_txt("materials/output_actions_inited_get_inited_list_games.txt")

    list_games = output_actions_inited.transform_inited_games_to_list(df_games)
    assert list_games == expected_str

def test_transform_inited_games_to_calendar():

    df_games = pd.read_csv("materials/qGame.csv")
    df_games["TIME_GAME_LOCAL"] = pd.to_datetime(df_games["TIME_GAME_LOCAL"],format="%H:%M:%S").dt.time
    expected_str = read_txt("materials/output_actions_inited_get_inited_calendar_games.txt")

    calendar_games,firstgametimedict = output_actions_inited.transform_inited_games_to_calendar(df_games)
    assert calendar_games == expected_str
    assert firstgametimedict == {'1ere journee': '01/01 15h'}

def test_get_inited_parameters():
    
    # this test the function get_inited_parameters
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_games_opening = pd.read_csv("materials/qGame.csv")
    mock_df_games_opening["TIME_GAME_LOCAL"] = pd.to_datetime(mock_df_games_opening["TIME_GAME_LOCAL"],format="%H:%M:%S").dt.time
    mock_df_games_opened = pd.read_csv("materials/qGame_Remaining_AtDate.csv")
    mock_df_games_opened["TIME_GAME_LOCAL"] = pd.to_datetime(mock_df_games_opened["TIME_GAME_LOCAL"],format="%H:%M:%S").dt.time
    expected_param_dict = {
        'GAMEDAY_OPENING': '1ere journee', 
        'NB_GAMES_OPENING': 2, 
        'CALENDAR_GAMES_OPENING': read_txt("materials/output_actions_inited_get_inited_calendar_games.txt"), 
        'FIRSTGAMETIME_OPENING': '__L__WEEKDAY_1__L__ 01/01 15h',
        'LIST_GAMES_OPENING': read_txt("materials/output_actions_inited_get_inited_list_games.txt"), 
        'NB_GAMES_OPENED': 2, 
        'LIST_GAMEDAYS_OPENED': '3eme journee , 4eme journee', 
        'CALENDAR_GAMES_OPENED': read_txt("materials/output_actions_inited_calendar_games_opened.txt"), 
        'LIST_GAMES_OPENED': read_txt("materials/output_actions_inited_list_games_opened.txt"),
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1}
    
    with patch('output_actions_inited.snowflake_execute', side_effect=[mock_df_games_opening,mock_df_games_opened]):
        param_dict = output_actions_inited.get_inited_parameters(sr_snowflake_account, sr_gameday_output_init)
        assert expected_param_dict == param_dict

def test_create_inited_message():
    
    # this test the function create_inited_message
    param_dict = {
        'GAMEDAY_OPENING': '1ere journee', 
        'NB_GAMES_OPENING': 2, 
        'CALENDAR_GAMES_OPENING': read_txt("materials/output_actions_inited_get_inited_calendar_games.txt"), 
        'FIRSTGAMETIME_OPENING': '__L__WEEKDAY_1__L__ 01/01 15h',
        'LIST_GAMES_OPENING': read_txt("materials/output_actions_inited_get_inited_list_games.txt"), 
        'NB_GAMES_OPENED': 2, 
        'LIST_GAMEDAYS_OPENED': '3eme journee , 4eme journee', 
        'CALENDAR_GAMES_OPENED': read_txt("materials/output_actions_inited_calendar_games_opened.txt"), 
        'LIST_GAMES_OPENED': read_txt("materials/output_actions_inited_list_games_opened.txt"),
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP':1}
    
    template = read_txt("materials/output_gameday_init_template_france.txt")
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
        'str_output_gameday_init_template_FRANCE': read_txt("materials/output_gameday_init_template_france.txt"),
        'str_output_gameday_init_template_ITALIA': read_txt("materials/output_gameday_init_template_france.txt")
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
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_list))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_calendar))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_parameters))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_message))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_inited))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
