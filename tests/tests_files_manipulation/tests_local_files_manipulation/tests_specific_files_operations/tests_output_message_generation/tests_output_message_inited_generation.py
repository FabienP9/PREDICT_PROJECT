'''
This tests file concern all functions in the output_message_inited_generation module.
It units test the happy path for each function
'''

from unittest.mock import patch
import pandas as pd

from src.predict_core.files_manipulation.local_files_manipulation.specific_files_operations.output_message_file_generation import output_message_inited_generation

def test_transform_games_to_list(read_csv,read_txt):

    # this test the function transform_games_to_list
    df_games = read_csv("q_vw_game_query.csv")
    expected_str = read_txt("output_message_inited_list_games.txt")

    list_games = output_message_inited_generation.transform_games_to_list(df_games)
    assert list_games.split() == expected_str.split()

def test_transform_databasetime_for_output():

    # this test the function transform_databasetime_for_output
    df_cols_time = pd.Series(["15:09:10", "09:00:00"])
    expected = pd.Series(["15h09","9h"])

    result = output_message_inited_generation.transform_databasetime_for_output(df_cols_time)
    pd.testing.assert_series_equal(result, expected, check_names=False)

def test_transform_databasedate_for_output():

    # this test the function transform_databasedate_for_output
    df_date = pd.Series("2026-02-01")
    expected = pd.Series("01/02")

    result = output_message_inited_generation.transform_databasedate_for_output(df_date)
    pd.testing.assert_series_equal(result, expected, check_names=False)

def test_add_time_to_databasedatetime():
    #this test the function add_time_to_databasedatetime

    df_col_date = pd.Series('2026-01-08')
    df_col_time = pd.Series('23:58:57')
    minutes_to_add = 3
    exp_date = pd.Series('2026-01-09')
    exp_time = pd.Series('00:01:57')

    date_new_col_date, date_new_col_time = output_message_inited_generation.add_time_to_databasedatetime(df_col_date,df_col_time,minutes_to_add)
    pd.testing.assert_series_equal(exp_date, date_new_col_date)
    pd.testing.assert_series_equal(exp_time, date_new_col_time)

def test_transform_games_to_calendar(read_csv,read_txt):

    df_games = read_csv("q_vw_game_query.csv")
    expected_str = read_txt("output_message_inited_calendar_games.txt")

    calendar_games, firstgametimedict = output_message_inited_generation.transform_games_to_calendar(df_games)
    assert calendar_games == expected_str
    assert firstgametimedict == {'1ere journee': '01/01 15h'}

def test_get_next_opening_gamedays_calendar(read_yml_as_serie, read_csv,read_txt):

    # this test the function get_next_opening_gamedays_calendar
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    mock_df_next_gamedays = read_csv("q_vw_gameday_nextopening_query.csv")
    expected_result = read_txt("output_message_inited_next_opening_gamedays_calendar.txt")
    
    with patch.object(output_message_inited_generation,'snowflake_execute', return_value=mock_df_next_gamedays):

        result, nbgamedays = output_message_inited_generation.get_next_opening_gamedays_calendar(sr_snowflake_account_connect, sr_gameday_output_init)
        assert result == expected_result
        assert nbgamedays == 2
        
def test_get_parameters(read_yml_as_serie, read_csv,read_txt):
    
    # this test the function get_parameters
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    mock_df_games_opening = read_csv("q_vw_game_query.csv")
    mock_df_games_opened = read_csv("q_vw_game_opened_atdate.csv")
    mock_calendar_next_opening = read_txt("output_message_inited_next_opening_gamedays_calendar.txt")
    
    expected_param_dict = {
        'GAMEDAY_OPENING': '1ere journee', 
        'NB_GAMES_OPENING': 2, 
        'CALENDAR_GAMES_OPENING': read_txt("output_message_inited_calendar_games.txt"), 
        'FIRSTGAMETIME_OPENING': '__L__WEEKDAY_1__L__ 01/01 15h',
        'LIST_GAMES_OPENING': read_txt("output_message_inited_list_games.txt"), 
        'NB_GAMES_OPENED': 2, 
        'LIST_GAMEDAYS_OPENED': '3eme journee , 4eme journee', 
        'CALENDAR_GAMES_OPENED': read_txt("output_message_inited_calendar_games_opened.txt"), 
        'LIST_GAMES_OPENED': read_txt("output_message_inited_list_games_opened.txt"),
        'CALENDAR_NEXT_OPENING': read_txt("output_message_inited_next_opening_gamedays_calendar.txt"),
        'NB_NEXT_OPENING' : 2,
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP': 1}
    
    with patch.object(output_message_inited_generation,'snowflake_execute', side_effect=[mock_df_games_opening,mock_df_games_opened]), \
        patch.object(output_message_inited_generation,'get_next_opening_gamedays_calendar', return_value=(mock_calendar_next_opening,2)):
        
        param_dict = output_message_inited_generation.get_parameters(sr_snowflake_account_connect, sr_gameday_output_init)
        assert expected_param_dict == param_dict

def test_create_message(read_csv,read_txt, read_json):
    
    # this test the function create_message
    param_dict = {
        'GAMEDAY_OPENING': '1ere journee', 
        'NB_GAMES_OPENING': 2, 
        'CALENDAR_GAMES_OPENING': read_txt("output_message_inited_calendar_games.txt"), 
        'FIRSTGAMETIME_OPENING': '__L__WEEKDAY_1__L__ 01/01 15h',
        'LIST_GAMES_OPENING': read_txt("output_message_inited_list_games.txt"), 
        'NB_GAMES_OPENED': 2, 
        'LIST_GAMEDAYS_OPENED': '3eme journee , 4eme journee', 
        'CALENDAR_GAMES_OPENED': read_txt("output_message_inited_calendar_games_opened.txt"), 
        'LIST_GAMES_OPENED': read_txt("output_message_inited_list_games_opened.txt"),
        'CALENDAR_NEXT_OPENING': read_txt("output_message_inited_next_opening_gamedays_calendar.txt"),
        'NB_NEXT_OPENING' : 2,
        'USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP':1}
    
    template = read_txt("output_gameday_init_template_france.txt")
    translations = read_json("output_gameday_template_translations.json")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    mock_filename = "result.txt"
    expected_result = read_txt("forumoutput_inited_s1_1erejournee_france_bi.txt")

    with patch.object(output_message_inited_generation.output,"define_filename", return_value=mock_filename) as mock_filename, \
         patch.object(output_message_inited_generation.files_manipulation,"create_txt"):
        
        content, country, forum = output_message_inited_generation.create_message(param_dict, template, translations, country, forum, sr_gameday_output_init)
        assert content.split() == expected_result.split()
        assert country == "FRANCE"
        assert forum == 'BI'

def test_process_output_message_inited(read_yml_as_serie, read_csv,read_txt, read_json):
    
    # this test the function process_output_message_inited
    context_dict = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'str_output_gameday_init_template_france': read_txt("output_gameday_init_template_france.txt"),
        'str_output_gameday_init_template_italia': read_txt("output_gameday_init_template_france.txt"),
        'lst_output_gameday_template_translations': read_json("output_gameday_template_translations.json")
    }
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    mock_df_topics = read_csv("q_vw_topic_init_query.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}
    mock_messages = [("fake_content_FRANCE", "FRANCE", "BI"), ("fake_content_ITALIA", "ITALIA", "II")]

    with patch.object(output_message_inited_generation,"snowflake_execute", return_value=mock_df_topics), \
         patch.object(output_message_inited_generation,"get_parameters", return_value=mock_params_retrieved), \
         patch.object(output_message_inited_generation,"multithread_run", return_value=mock_messages):

        output_message_inited_generation.process_output_message_inited(context_dict, sr_gameday_output_init)
