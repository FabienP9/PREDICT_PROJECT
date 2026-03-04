'''
This tests file concern all functions in the output_actions_inited module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from output_message_generation_actions import output_message_actions_inited as output_message_actions_inited
from testutils import read_txt
from testutils import read_json

def test_transform_inited_games_to_list():

    # this test the function transform_inited_games_to_list
    df_games = pd.read_csv("materials/qGame.csv")
    expected_str = read_txt("materials/output_message_inited_list_games.txt")

    list_games = output_message_actions_inited.transform_inited_games_to_list(df_games)
    assert list_games.split() == expected_str.split()

def test_transform_databasetime_for_output():

    # this test the function transform_databasetime_for_output
    df_cols_time = pd.Series(["15:09:10", "09:00:00"])
    expected = pd.Series(["15h09","9h"])

    result = output_message_actions_inited.transform_databasetime_for_output(df_cols_time)
    pd.testing.assert_series_equal(result, expected, check_names=False)

def test_transform_databasedate_for_output():

    # this test the function transform_databasedate_for_output
    df_date = pd.Series("2026-02-01")
    expected = pd.Series("01/02")

    result = output_message_actions_inited.transform_databasedate_for_output(df_date)
    pd.testing.assert_series_equal(result, expected, check_names=False)

def test_add_time_to_databasedatetime():
    #this test the function add_time_to_databasedatetime

    df_col_date = pd.Series('2026-01-08')
    df_col_time = pd.Series('23:58:57')
    minutes_to_add = 3
    exp_date = pd.Series('2026-01-09')
    exp_time = pd.Series('00:01:57')

    date_new_col_date, date_new_col_time = output_message_actions_inited.add_time_to_databasedatetime(df_col_date,df_col_time,minutes_to_add)
    pd.testing.assert_series_equal(exp_date, date_new_col_date)
    pd.testing.assert_series_equal(exp_time, date_new_col_time)

def test_transform_inited_games_to_calendar():

    df_games = pd.read_csv("materials/qGame.csv")
    expected_str = read_txt("materials/output_message_inited_calendar_games.txt")

    calendar_games, firstgametimedict = output_message_actions_inited.transform_inited_games_to_calendar(df_games)
    assert calendar_games == expected_str
    assert firstgametimedict == {'1ere journee': '01/01 15h'}

def test_get_inited_next_opening_gamedays_calendar():

    # this test the function get_inited_next_opening_gamedays_calendar
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mocK_df_next_gamedays = pd.read_csv("materials/qNextGamedaysOpening.csv")
    expected_result = read_txt("materials/output_message_inited_next_opening_gamedays_calendar.txt")
    
    with patch.object(output_message_actions_inited,'snowflake_execute', return_value=mocK_df_next_gamedays):

        result, nbgamedays = output_message_actions_inited.get_inited_next_opening_gamedays_calendar(sr_snowflake_account, sr_gameday_output_init)
        assert result == expected_result
        assert nbgamedays == 2
        
def test_get_inited_parameters():
    
    # this test the function get_inited_parameters
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_games_opening = pd.read_csv("materials/qGame.csv")
    mock_df_games_opened = pd.read_csv("materials/qGame_Remaining_AtDate.csv")
    mock_calendar_next_opening = read_txt("materials/output_message_inited_next_opening_gamedays_calendar.txt")
    
    expected_param_dict = {
        'GAMEDAY_OPENING': '1ere journee', 
        'NB_GAMES_OPENING': 2, 
        'CALENDAR_GAMES_OPENING': read_txt("materials/output_message_inited_calendar_games.txt"), 
        'FIRSTGAMETIME_OPENING': '__L__WEEKDAY_1__L__ 01/01 15h',
        'LIST_GAMES_OPENING': read_txt("materials/output_message_inited_list_games.txt"), 
        'NB_GAMES_OPENED': 2, 
        'LIST_GAMEDAYS_OPENED': '3eme journee , 4eme journee', 
        'CALENDAR_GAMES_OPENED': read_txt("materials/output_message_inited_calendar_games_opened.txt"), 
        'LIST_GAMES_OPENED': read_txt("materials/output_message_inited_list_games_opened.txt"),
        'CALENDAR_NEXT_OPENING': read_txt("materials/output_message_inited_next_opening_gamedays_calendar.txt"),
        'NB_NEXT_OPENING' : 2,
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1}
    
    with patch.object(output_message_actions_inited,'snowflake_execute', side_effect=[mock_df_games_opening,mock_df_games_opened]), \
        patch.object(output_message_actions_inited,'get_inited_next_opening_gamedays_calendar', return_value=(mock_calendar_next_opening,2)):
        
        param_dict = output_message_actions_inited.get_inited_parameters(sr_snowflake_account, sr_gameday_output_init)
        assert expected_param_dict == param_dict

def test_create_inited_message():
    
    # this test the function create_inited_message
    param_dict = {
        'GAMEDAY_OPENING': '1ere journee', 
        'NB_GAMES_OPENING': 2, 
        'CALENDAR_GAMES_OPENING': read_txt("materials/output_message_inited_calendar_games.txt"), 
        'FIRSTGAMETIME_OPENING': '__L__WEEKDAY_1__L__ 01/01 15h',
        'LIST_GAMES_OPENING': read_txt("materials/output_message_inited_list_games.txt"), 
        'NB_GAMES_OPENED': 2, 
        'LIST_GAMEDAYS_OPENED': '3eme journee , 4eme journee', 
        'CALENDAR_GAMES_OPENED': read_txt("materials/output_message_inited_calendar_games_opened.txt"), 
        'LIST_GAMES_OPENED': read_txt("materials/output_message_inited_list_games_opened.txt"),
        'CALENDAR_NEXT_OPENING': read_txt("materials/output_message_inited_next_opening_gamedays_calendar.txt"),
        'NB_NEXT_OPENING' : 2,
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP':1}
    
    template = read_txt("materials/output_gameday_init_template_france.txt")
    translations = read_json("materials/output_gameday_template_translations.json")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_filename = "result.txt"
    expected_result = read_txt("materials/forumoutput_inited_s1_1erejournee_france_bi.txt")

    with patch.object(output_message_actions_inited.outputA,"define_filename", return_value=mock_filename) as mock_filename, \
         patch.object(output_message_actions_inited.fileA,"create_txt"):
        
        content, country, forum = output_message_actions_inited.create_inited_message(param_dict, template, translations, country, forum, sr_gameday_output_init)
        assert content.split() == expected_result.split()
        assert country == "FRANCE"
        assert forum == 'BI'

def test_process_output_message_inited():
    
    # this test the function process_output_message_inited
    context_dict = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'str_output_gameday_init_template_FRANCE': read_txt("materials/output_gameday_init_template_france.txt"),
        'str_output_gameday_init_template_ITALIA': read_txt("materials/output_gameday_init_template_france.txt"),
        'lst_output_gameday_template_translations': read_json("materials/output_gameday_template_translations.json")
    }
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_topics = pd.read_csv("materials/qTopics_Init.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}
    mock_messages = [("fake_content_FRANCE", "FRANCE", "BI"), ("fake_content_ITALIA", "ITALIA", "II")]

    with patch.object(output_message_actions_inited,"snowflake_execute", return_value=mock_df_topics), \
         patch.object(output_message_actions_inited,"get_inited_parameters", return_value=mock_params_retrieved), \
         patch.object(output_message_actions_inited.config,"multithreading_run", return_value=mock_messages), \
         patch.object(output_message_actions_inited,"post_message"):

        output_message_actions_inited.process_output_message_inited(context_dict, sr_gameday_output_init)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_list))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_databasetime_for_output))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_databasedate_for_output))
    test_suite.addTest(unittest.FunctionTestCase(test_add_time_to_databasedatetime))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_calendar))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_next_opening_gamedays_calendar))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_parameters))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_message))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_inited))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
