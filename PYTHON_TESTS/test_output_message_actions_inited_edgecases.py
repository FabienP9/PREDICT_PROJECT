'''
This tests file concern all functions in the output_actions_inited module.
It units test unexpected paths
'''
import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from output_message_generation_actions import output_message_actions_inited as output_message_actions_inited
from testutils import assertExit
from testutils import read_txt
from testutils import read_json

def test_transform_inited_games_to_list_empty():
    
    # this test the function transform_inited_games_to_listwith an empty dataframe. Must result in an empty string
    df_games = pd.read_csv("materials/edgecases/qGame_empty.csv")

    result = output_message_actions_inited.transform_inited_games_to_list(df_games)
    assert result == ""

def test_transform_inited_games_to_list_missing_column():

    # this test the function transform_inited_games_to_list with a missing column. Must exit the program
    df_games = pd.read_csv("materials/edgecases/qGame_missing_game_message.csv")
    assertExit(lambda: output_message_actions_inited.transform_inited_games_to_list(df_games))

def test_transform_databasetime_for_output_invalid_format():

    # this test the function transform_databasetime_for_output with invalid dormat. Must accept it, as we don't need second. But should not happen due to format check in databses
    df_cols_time = pd.Series(["15:30"])
    expected = pd.Series(["15h30"])

    result = output_message_actions_inited.transform_databasetime_for_output(df_cols_time)
    pd.testing.assert_series_equal(result, expected, check_names=False)

def test_transform_databasetime_for_output_None():

    # this test the function transform_databasetime_for_output with None. Must exit the program.
    df_cols_time = pd.Series([None])
    assertExit(lambda: output_message_actions_inited.transform_databasetime_for_output(df_cols_time))

def test_transform_databasedate_for_output_invalid_format():

    # this test the function transform_databasedate_for_output with invalid format. Must exit program
    df_date = pd.Series(["2024/01/31"])
    assertExit(lambda: output_message_actions_inited.transform_databasedate_for_output(df_date))

def test_transform_databasedate_for_output_noday():

    # this test the function transform_databasedate_for_output with no day. Must exit the program.
    df_date = pd.Series("2026-02")
    assertExit(lambda: output_message_actions_inited.transform_databasedate_for_output(df_date))

def test_add_time_to_databasedatetime_badformat():
    #this test the function add_time_to_databasedatetime with bad format for time.Must exit the program

    df_col_date = pd.Series('2026-01-08')
    df_col_time = pd.Series('99:99:99')
    minutes_to_add = 3

    assertExit(lambda: output_message_actions_inited.add_time_to_databasedatetime(df_col_date,df_col_time,minutes_to_add))

def test_transform_inited_games_to_calendar_empty():
    
    # this test the function transform_inited_games_to_calendar with an empty dataframe. Must return an empty result.
    df_games = pd.read_csv("materials/edgecases/qGame_empty.csv")
    calendar_games, firstgametimedict = output_message_actions_inited.transform_inited_games_to_calendar(df_games)
    assert calendar_games == ""
    assert firstgametimedict == {}

def test_transform_inited_games_to_calendar_bad_type():
    
    # this test the function transform_inited_games_to_calendar with a string instead of time. Must exit the program, but should not happen due to test in database.
    df_games = pd.read_csv("materials/edgecases/qGame_badtype.csv")
    assertExit(lambda: output_message_actions_inited.transform_inited_games_to_calendar(df_games))

def test_get_inited_next_opening_gamedays_calendar_empty():

    # this test the function get_inited_next_opening_gamedays_calendar with an empty dataframe. Must return an empty string
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mocK_df_next_gamedays = pd.read_csv("materials/edgecases/qNextGamedaysOpening_empty.csv")
    
    with patch.object(output_message_actions_inited,'snowflake_execute', return_value=mocK_df_next_gamedays):

        result, nbgamedays = output_message_actions_inited.get_inited_next_opening_gamedays_calendar(sr_snowflake_account, sr_gameday_output_init)
        assert result == ""
        assert nbgamedays == 0

def test_get_inited_next_opening_gamedays_calendar_badtype():

    # this test the function get_inited_next_opening_gamedays_calendar with badtype. Must exit the program
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mocK_df_next_gamedays = pd.read_csv("materials/edgecases/qNextGamedaysOpening_badtype.csv")
    
    with patch.object(output_message_actions_inited,'snowflake_execute', return_value=mocK_df_next_gamedays):

        assertExit(lambda: output_message_actions_inited.get_inited_next_opening_gamedays_calendar(sr_snowflake_account, sr_gameday_output_init))

def test_create_inited_message_no_remaining_games():
    
    # this test the function create_inited_message with NB_GAMES_OPENED = 0 and NB_NEXT_OPENING=0. Must not wrtie the two last paragraphs
    param_dict = {
        'GAMEDAY_OPENING': '1ere journee', 
        'NB_GAMES_OPENING': 2, 
        'CALENDAR_GAMES_OPENING': read_txt("materials/output_message_inited_calendar_games.txt"), 
        'FIRSTGAMETIME_OPENING': '__L__WEEKDAY_1__L__ 01/01 15h',
        'LIST_GAMES_OPENING': read_txt("materials/output_message_inited_list_games.txt"), 
        'NB_GAMES_OPENED': 0, 
        'LIST_GAMEDAYS_OPENED': '', 
        'CALENDAR_GAMES_OPENED': '', 
        'LIST_GAMES_OPENED': '',        
        'CALENDAR_NEXT_OPENING': '',
        'NB_NEXT_OPENING' : 0,
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP':1
    }
    
    template = read_txt("materials/output_gameday_init_template_france.txt")
    translations = read_json("materials/output_gameday_template_translations.json")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_filename = "result.txt"
    expected_result = read_txt("materials/edgecases/forumoutput_inited_s1_1erejournee_france_no_gamedays.txt")

    with patch.object(output_message_actions_inited.outputA,"define_filename", return_value=mock_filename) as mock_filename, \
         patch.object(output_message_actions_inited.fileA,"create_txt"):
        
        
        content, country, forum = output_message_actions_inited.create_inited_message(param_dict, template, translations, country, forum, sr_gameday_output_init)
        assert content.split() == expected_result.split()
        assert country == "FRANCE"
        assert forum == 'BI'
        
def test_create_inited_message_none_param():
    
    # this test the function create_inited_message with no parameters. Must exit the program
    param_dict = None
    template = read_txt("materials/output_gameday_init_template_france.txt")
    translations = read_json("materials/output_gameday_template_translations.json")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    
    assertExit(lambda: output_message_actions_inited.create_inited_message(param_dict, template, translations, country, forum, sr_gameday_output_init))

def test_process_output_message_inited_with_no_topics():
    
    # this test the function process_output_message_inited with no topics provided. multithreading_run for posting should be called with empty list
    context_dict = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'str_output_gameday_init_template_FRANCE': read_txt("materials/output_gameday_init_template_france.txt"),
        'str_output_gameday_init_template_ITALIA': read_txt("materials/output_gameday_init_template_france.txt"),
        'lst_output_gameday_template_translations': read_json("materials/output_gameday_template_translations.json")
    }
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    mock_df_topics = pd.read_csv("materials/edgecases/qTopics_Init_empty.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}

    # Patch all external dependencies
    with patch.object(output_message_actions_inited,"snowflake_execute", return_value=mock_df_topics), \
         patch.object(output_message_actions_inited,"get_inited_parameters", return_value=mock_params_retrieved), \
         patch.object(output_message_actions_inited.config,"multithreading_run", return_value=[]) as mock_mt, \
         patch.object(output_message_actions_inited,"post_message"):

        # Call the function
        output_message_actions_inited.process_output_message_inited(context_dict, sr_gameday_output_init)
        # posting step should have been called once with []
        called_args = mock_mt.call_args_list[-1][0][1]
        assert called_args == []
        
if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_list_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_list_missing_column))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_databasetime_for_output_invalid_format))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_databasetime_for_output_None))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_databasedate_for_output_invalid_format))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_databasedate_for_output_noday))
    test_suite.addTest(unittest.FunctionTestCase(test_add_time_to_databasedatetime_badformat))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_calendar_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_inited_games_to_calendar_bad_type))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_next_opening_gamedays_calendar_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_get_inited_next_opening_gamedays_calendar_badtype))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_message_no_remaining_games))
    test_suite.addTest(unittest.FunctionTestCase(test_create_inited_message_none_param))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_inited_with_no_topics))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)