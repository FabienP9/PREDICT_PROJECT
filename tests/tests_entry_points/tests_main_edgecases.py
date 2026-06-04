'''
This tests file concern all functions in the main module.
It units test edge cases for functions
'''
from unittest.mock import patch
import pandas as pd

from src.predict_core.entry_point import main

def test_process_games_missing_key(assert_exit):
    
    # this test the process_games with missing key in context dict. Must exit the program.
    context = {}
    assert_exit(lambda: main.process_games(context))
    
def test_process_games_failure(read_csv,assert_exit):
    
    # this test the process_games with game_list_games_from_need failing. Must exit the program.
    context = {
        'sr_output_need': read_csv("output_need_calculate.csv").iloc[0],
        'df_competition': read_csv("competition_unique.csv"),
        "df_paths" : read_csv("paths.csv"),
        "df_gameday_modification":pd.DataFrame()
    }

    with patch.object(main.games_details_extraction,"extract_games_from_need", side_effect=RuntimeError("boom")), \
         patch.object(main.files_manipulation,"filter_data"):

        assert_exit(lambda: main.process_games(context))

def test_process_messages_extract_messages_failure(read_csv, read_yml_as_serie, assert_exit):
    
    # this test the process_messages with extract_messages failing. Must exit the program.
    context = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'sr_output_need': read_csv("output_need_calculate.csv").iloc[0],
        "df_paths" : read_csv("paths.csv"),
        "df_boolean_check_message_manually" : read_csv("boolean_check_message_manually_0.csv"),
        "df_message_check_ts" : read_csv("message_check_ts.csv"),      
    }

    with patch.object(main.messages_details_extraction,"extract_messages", side_effect=ValueError("bad")):

            assert_exit(lambda: main.process_messages(context))
        
def test_process_messages_invalid_message_dataframe(read_yml_as_serie, read_csv, assert_exit):
    
    # this test the process_messages with invalid message. Must exit the program.
    context = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'sr_output_need': read_csv("output_need_calculate.csv").iloc[0],
        "df_paths" : read_csv("paths.csv"),
        "df_boolean_check_message_manually" : read_csv("boolean_check_message_manually_0.csv"),
        "df_message_check_ts" : read_csv("message_check_ts.csv"),      
    }
    df_message_check = pd.DataFrame({"WRONG_COL": ["a", "b"]})
    extraction_time_utc = '2025-01-01 18:00:00'

    with patch.object(main.messages_details_extraction,"extract_messages",return_value=(df_message_check,extraction_time_utc)), \
         patch.object(main.files_manipulation,"filter_data", return_value={}), \
         patch.object(main.output_need_calculation,"set_output_need_to_check_status"), \
         patch.object(main.files_manipulation,"create_csv"):

            assert_exit(lambda: main.process_messages(context))

def test_display_check_string_missing_key(read_yml_as_serie, read_csv,assert_exit):
    
    # this test the display_check_string with missing columns in sr_output_need. Must exit the program.
    context = {
        'sr_output_need': {},
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'extraction_time_utc':  "2000-01-02 01:00:00",
        'nb_new_messages': 3,
        "df_boolean_check_message_manually" : read_csv("boolean_check_message_manually_1.csv")
    }
    assert_exit(lambda: main.display_check_string(context))
    
def test_main_dropbox_failure(assert_exit):
    
    # this test the main with dropbox connection failing. Must exit the program.
    with patch.object(main.env,"check_environment_variable"),\
         patch.object(main.dropbox,"initiate_folder", side_effect=OSError("dropbox fail")):
        assert_exit(lambda: main.main())
    
def test_main_generate_output_need_failure(read_csv, read_yml_as_serie, assert_exit):
    
    # this test the main with generate_output_need failing. Must exit the program.
    mock_initiate_local_dict = {
         "df_paths": read_csv("paths.csv"),
         "sr_output_need" : read_csv("output_need_calculate.csv").iloc[0],
         "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
         'df_task_done' : read_csv("task_done.csv")
    }

    with patch.object(main.env,"check_environment_variable"),\
         patch.object(main.dropbox,"initiate_folder"), \
         patch.object(main.local_environment_manipulation,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(main.output_need_calculation,"generate_output_need", side_effect=Exception("gen fail")):
        
        assert_exit(lambda: main.main())
