'''
This tests file concern all functions in the exe_main module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch, mock_open
import pandas as pd
import os
import sys
import json
from pandas.testing import assert_frame_equal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_main 

def test_process_games():
    
    # this test the process_games function
    context = {
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0],
        'df_competition': pd.read_csv("materials/competition_unique.csv"),
        "df_paths" : pd.read_csv("materials/paths.csv")
    }

    mock_df_game = pd.read_csv("materials/game.csv")

    with patch("exe_main.gameA.extract_games_from_need",return_value=mock_df_game), \
         patch("exe_main.fileA.filter_data"):

        result = exe_main.process_games(context)
        assert "df_game" in result

def test_process_messages_autoprocess():
    
    # this test the process_messages function with an automatic process
    context = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'sr_output_need': pd.read_csv("materials/output_need_check_with_message_check_ts.csv").iloc[0],
        "df_paths" : pd.read_csv("materials/paths.csv"),
        "df_boolean_check_message_manually" : pd.read_csv("materials/boolean_check_message_manually_0.csv"),
        "df_message_check_ts" : pd.read_csv("materials/message_check_ts.csv"),      
    }
    df_message_check = pd.read_csv("materials/message_check.csv")
    extraction_time_utc = '2025-01-01 18:00:00'

    with patch("exe_main.extract_messages",return_value=(df_message_check,extraction_time_utc)), \
         patch("exe_main.fileA.filter_data", return_value={}), \
         patch("exe_main.set_output_need_to_check_status"), \
         patch("exe_main.fileA.create_csv"), \
         patch("exe_main.fileA.create_csv"):

            result = exe_main.process_messages(context)
            assert "df_message_check" in result
            assert "extraction_time_utc" in result
            assert "df_message_check_ts" in result
            assert (result['df_message_check_ts'].loc[result['df_message_check_ts']['SEASON_ID'] == context['sr_output_need']['SEASON_ID'], 'LAST_CHECK_TS_UTC'] == extraction_time_utc).all()

def test_process_messages_manualprocess():
    
    # this test the process_messages function with an manual process
    context = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'sr_output_need': pd.read_csv("materials/output_need_check_with_message_check_ts.csv").iloc[0],
        "df_paths" : pd.read_csv("materials/paths.csv"),
        "df_boolean_check_message_manually" : pd.read_csv("materials/boolean_check_message_manually_1.csv"),
        "df_message_check_ts" : pd.read_csv("materials/message_check_ts.csv"),      
    }
    df_message_check = pd.read_csv("materials/message_check.csv")
    extraction_time_utc = '2025-01-01 18:00:00'

    with patch("exe_main.extract_messages",return_value=(df_message_check,extraction_time_utc)), \
         patch("exe_main.fileA.filter_data", return_value={}), \
         patch("exe_main.set_output_need_to_check_status"), \
         patch("exe_main.fileA.create_csv"), \
         patch("exe_main.fileA.create_csv"):

            result = exe_main.process_messages(context)
            assert "df_message_check" in result
            assert "extraction_time_utc" in result
            assert "df_message_check_ts" in result
            assert_frame_equal(result['df_message_check_ts'].reset_index(drop=True), context['df_message_check_ts'].reset_index(drop=True))

def test_display_check_string_with_messages():
    
    # this test the display_check_string function with messages - manual processing
    context = {
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0],
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'extraction_time_utc':  "2000-01-02 01:00:00",
        'nb_new_messages': 3,
        "df_boolean_check_message_manually" : pd.read_csv("materials/boolean_check_message_manually_1.csv")
    }
    context['sr_output_need']['TASK_RUN']  = "CHECK"
    context['sr_output_need']['LAST_MESSAGE_CHECK_TS_UTC']  = "2000-01-01 00:01:00"
    check_string = exe_main.display_check_string(context)
    assert check_string.strip() != ""
    assert check_string is not None
    assert check_string != "No need to check - no new messages"

def test_display_check_string_without_messages():
    
    # this test the display_check_string function without messages - manual processing
    context = {
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0],
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'extraction_time_utc':  "2000-01-02 01:00:00",
        'nb_new_messages': 0,
        "df_boolean_check_message_manually" : pd.read_csv("materials/boolean_check_message_manually_1.csv")
    }
    context['sr_output_need']['TASK_RUN']  = "CHECK"
    context['sr_output_need']['LAST_MESSAGE_CHECK_TS_UTC']  = "2000-01-01 00:01:00"
    check_string = exe_main.display_check_string(context)
    assert check_string == "No need to check - no new messages"

def test_display_check_string_autoprocess():
    
    # this test the display_check_string function with auto processing
    context = {
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0],
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'extraction_time_utc':  "2000-01-02 01:00:00",
        'nb_new_messages': 0,
        "df_boolean_check_message_manually" : pd.read_csv("materials/boolean_check_message_manually_0.csv")
    }
    context['sr_output_need']['TASK_RUN']  = "CHECK"
    check_string = exe_main.display_check_string(context)
    assert check_string == "No need to check - messages processed automatically"

def test_exe_main():
    
    # this test the exe_main function with all inputs mocked
    mock_initiate_local_dict = {
         "df_paths": pd.read_csv("materials/paths.csv"),
         "sr_output_need" : pd.read_csv("materials/output_need_calculate.csv").iloc[0],
         'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
         'df_task_done' : pd.read_csv("materials/task_done.csv"),
         "df_boolean_check_message_manually" : pd.read_csv("materials/boolean_check_message_manually_0.csv")
    }
    mock_sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]
    mock_extraction_time_utc = '2025-01-01 18:00:00'
    m = mock_open()

    with patch("exe_main.dropbox_initiate_folder"), \
         patch("exe_main.fileA.initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch("exe_main.generate_output_need", return_value=mock_sr_output_need), \
         patch("exe_main.fileA.download_needed_files",return_value = {}),\
         patch("exe_main.process_games",return_value = mock_initiate_local_dict), \
         patch("exe_main.process_messages",return_value = mock_initiate_local_dict), \
         patch("exe_main.snowflakeA.delete_tables_data_from_python"), \
         patch("exe_main.snowflakeA.update_snowflake"), \
         patch("exe_main.update_calendar_related_files",return_value = mock_extraction_time_utc), \
         patch("exe_main.outputA.generate_output_message"), \
         patch("exe_init_compet.fileA.terminate_local_environment"), \
         patch("builtins.open", m):

        exe_main.exe_main()
        m.assert_called_once_with("exe_output.json", "w")

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_process_games))
    test_suite.addTest(unittest.FunctionTestCase(test_process_messages_autoprocess))
    test_suite.addTest(unittest.FunctionTestCase(test_process_messages_manualprocess))
    test_suite.addTest(unittest.FunctionTestCase(test_display_check_string_with_messages))
    test_suite.addTest(unittest.FunctionTestCase(test_display_check_string_without_messages))
    test_suite.addTest(unittest.FunctionTestCase(test_display_check_string_autoprocess))
    test_suite.addTest(unittest.FunctionTestCase(test_exe_main))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
