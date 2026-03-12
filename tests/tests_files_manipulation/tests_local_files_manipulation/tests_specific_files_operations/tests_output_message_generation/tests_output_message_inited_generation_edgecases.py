'''
This tests file concern all functions in the output_message_inited_generation module.
It units test unexpected paths
'''
import pandas as pd
from unittest.mock import patch

from src.predict_core.files_manipulation.local_files_manipulation.specific_files_operations.output_message_file_generation import output_message_inited_generation

def test_transform_games_to_list_empty(read_csv):
    
    # this test the function transform_games_to_listwith an empty dataframe. Must result in an empty string
    df_games = read_csv("edgecases/q_vw_game_query_empty.csv")

    result = output_message_inited_generation.transform_games_to_list(df_games)
    assert result == ""

def test_transform_games_to_list_missing_column(read_csv,assert_exit):

    # this test the function transform_games_to_list with a missing column. Must exit the program
    df_games = read_csv("edgecases/q_vw_game_query_missing_game_message.csv")
    assert_exit(lambda: output_message_inited_generation.transform_games_to_list(df_games))

def test_transform_databasetime_for_output_invalid_format():

    # this test the function transform_databasetime_for_output with invalid format. Must accept it, as we don't need second. But should not happen due to format check in databses
    df_cols_time = pd.Series(["15:30"])
    expected = pd.Series(["15h30"])

    result = output_message_inited_generation.transform_databasetime_for_output(df_cols_time)
    pd.testing.assert_series_equal(result, expected, check_names=False)

def test_transform_databasetime_for_output_none(assert_exit):

    # this test the function transform_databasetime_for_output with None. Must exit the program.
    df_cols_time = pd.Series([None])
    assert_exit(lambda: output_message_inited_generation.transform_databasetime_for_output(df_cols_time))

def test_transform_databasedate_for_output_invalid_format(assert_exit):

    # this test the function transform_databasedate_for_output with invalid format. Must exit program
    df_date = pd.Series(["2024/01/31"])
    assert_exit(lambda: output_message_inited_generation.transform_databasedate_for_output(df_date))

def test_transform_databasedate_for_output_noday(assert_exit):

    # this test the function transform_databasedate_for_output with no day. Must exit the program.
    df_date = pd.Series("2026-02")
    assert_exit(lambda: output_message_inited_generation.transform_databasedate_for_output(df_date))

def test_add_time_to_databasedatetime_badformat(assert_exit):
    #this test the function add_time_to_databasedatetime with bad format for time.Must exit the program

    df_col_date = pd.Series('2026-01-08')
    df_col_time = pd.Series('99:99:99')
    minutes_to_add = 3

    assert_exit(lambda: output_message_inited_generation.add_time_to_databasedatetime(df_col_date,df_col_time,minutes_to_add))

def test_transform_games_to_calendar_empty(read_csv):
    
    # this test the function transform_games_to_calendar with an empty dataframe. Must return an empty result.
    df_games = read_csv("edgecases/q_vw_game_query_empty.csv")
    calendar_games, firstgametimedict = output_message_inited_generation.transform_games_to_calendar(df_games)
    assert calendar_games == ""
    assert firstgametimedict == {}

def test_transform_games_to_calendar_bad_type(read_csv, assert_exit):
    
    # this test the function transform_games_to_calendar with a string instead of time. Must exit the program, but should not happen due to test in database.
    df_games = read_csv("edgecases/q_vw_game_query_badtype.csv")
    assert_exit(lambda: output_message_inited_generation.transform_games_to_calendar(df_games))

def test_get_next_opening_gamedays_calendar_empty(read_csv):

    # this test the function get_next_opening_gamedays_calendar with an empty dataframe. Must return an empty string
    sr_snowflake_account = read_csv("snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    mock_df_next_gamedays = read_csv("edgecases/q_vw_gameday_nextopening_query_empty.csv")
    
    with patch.object(output_message_inited_generation,'snowflake_execute', return_value=mock_df_next_gamedays):

        result, nbgamedays = output_message_inited_generation.get_next_opening_gamedays_calendar(sr_snowflake_account, sr_gameday_output_init)
        assert result == ""
        assert nbgamedays == 0

def test_get_next_opening_gamedays_calendar_badtype(read_csv, assert_exit):

    # this test the function get_next_opening_gamedays_calendar with badtype. Must exit the program
    sr_snowflake_account = read_csv("snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    mock_df_next_gamedays = read_csv("edgecases/q_vw_gameday_nextopening_query_badtype.csv")
    
    with patch.object(output_message_inited_generation,'snowflake_execute', return_value=mock_df_next_gamedays):

        assert_exit(lambda: output_message_inited_generation.get_next_opening_gamedays_calendar(sr_snowflake_account, sr_gameday_output_init))

def test_create_message_no_remaining_games(read_csv, read_txt, read_json, assert_exit):
    
    # this test the function create_message with NB_GAMES_OPENED = 0 and NB_NEXT_OPENING=0. Must not wrtie the two last paragraphs
    param_dict = {
        'GAMEDAY_OPENING': '1ere journee', 
        'NB_GAMES_OPENING': 2, 
        'CALENDAR_GAMES_OPENING': read_txt("output_message_inited_calendar_games.txt"), 
        'FIRSTGAMETIME_OPENING': '__L__WEEKDAY_1__L__ 01/01 15h',
        'LIST_GAMES_OPENING': read_txt("output_message_inited_list_games.txt"), 
        'NB_GAMES_OPENED': 0, 
        'LIST_GAMEDAYS_OPENED': '', 
        'CALENDAR_GAMES_OPENED': '', 
        'LIST_GAMES_OPENED': '',        
        'CALENDAR_NEXT_OPENING': '',
        'NB_NEXT_OPENING' : 0,
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP':1
    }
    
    template = read_txt("output_gameday_init_template_france.txt")
    translations = read_json("output_gameday_template_translations.json")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    mock_filename = "result.txt"
    expected_result = read_txt("edgecases/forumoutput_inited_s1_1erejournee_france_no_gamedays.txt")

    with patch.object(output_message_inited_generation.output,"define_filename", return_value=mock_filename) as mock_filename, \
         patch.object(output_message_inited_generation.files_manipulation,"create_txt"):
        
        
        content, country, forum = output_message_inited_generation.create_message(param_dict, template, translations, country, forum, sr_gameday_output_init)
        assert content.split() == expected_result.split()
        assert country == "FRANCE"
        assert forum == 'BI'
        
def test_create_message_none_param(read_csv, read_txt, read_json, assert_exit):
    
    # this test the function create_message with no parameters. Must exit the program
    param_dict = None
    template = read_txt("output_gameday_init_template_france.txt")
    translations = read_json("output_gameday_template_translations.json")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    
    assert_exit(lambda: output_message_inited_generation.create_message(param_dict, template, translations, country, forum, sr_gameday_output_init))

def test_process_output_message_with_no_topics(read_csv, read_txt, read_json):
    
    # this test the function process_output_message_inited with no topics provided. multithreading_run for posting should be called with empty list
    context_dict = {
        'sr_snowflake_account_connect': read_csv("snowflake_account_connect.csv").iloc[0],
        'str_output_gameday_init_template_france': read_txt("output_gameday_init_template_france.txt"),
        'str_output_gameday_init_template_italia': read_txt("output_gameday_init_template_france.txt"),
        'lst_output_gameday_template_translations': read_json("output_gameday_template_translations.json")
    }
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    mock_df_topics = read_csv("edgecases/q_vw_topic_init_query_empty.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}

    # Patch all external dependencies
    with patch.object(output_message_inited_generation,"snowflake_execute", return_value=mock_df_topics), \
         patch.object(output_message_inited_generation,"get_parameters", return_value=mock_params_retrieved), \
         patch.object(output_message_inited_generation,"multithread_run", return_value=[]) as mock_mt:

        # Call the function
        output_message_inited_generation.process_output_message_inited(context_dict, sr_gameday_output_init)
        # posting step should have been called once with []
        called_args = mock_mt.call_args_list[-1][0][1]
        assert called_args == []
 