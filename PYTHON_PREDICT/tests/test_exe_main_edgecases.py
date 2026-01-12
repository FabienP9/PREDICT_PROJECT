'''
This tests file concern all functions in the exe_main module.
It units test edge cases for functions
'''
import unittest
from unittest.mock import patch
import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_main
from testutils import assertExit

def test_process_games_missing_key():
    
    # this test the process_games with missing key in context dict. Must exit the program.
    context = {}
    assertExit(lambda: exe_main.process_games(context))
    
def test_process_games_gameA_failure():
    
    # this test the process_games with game_list_games_from_need failing. Must exit the program.
    context = {
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0],
        'df_competition': pd.read_csv("materials/competition_unique.csv"),
        "df_paths" : pd.read_csv("materials/paths.csv")
    }

    with patch("exe_main.gameA.extract_games_from_need", side_effect=RuntimeError("boom")), \
         patch("exe_main.fileA.filter_data"):

        assertExit(lambda: exe_main.process_games(context))

def test_process_messages_extract_messages_failure():
    
    # this test the process_messages with extract_messages failing. Must exit the program.
    context = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0],
        "df_paths" : pd.read_csv("materials/paths.csv"),
    }

    with patch("exe_main.extract_messages", side_effect=ValueError("bad")):

            assertExit(lambda: exe_main.process_messages(context))
        
def test_process_messages_invalid_message_dataframe():
    
    # this test the process_messages with invalid message. Must exit the program.
    context = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'sr_output_need': pd.read_csv("materials/output_need_calculate.csv").iloc[0],
        "df_paths" : pd.read_csv("materials/paths.csv"),
    }
    df_message_check = pd.DataFrame({"WRONG_COL": ["a", "b"]})
    extraction_time_utc = '2025-01-01 18:00:00'

    with patch("exe_main.extract_messages",return_value=(df_message_check,extraction_time_utc)), \
         patch("exe_main.fileA.filter_data", return_value={}), \
         patch("exe_main.set_output_need_to_check_status"), \
         patch("exe_main.fileA.create_csv"):

            assertExit(lambda: exe_main.process_messages(context))

def test_display_check_string_missing_key():
    
    # this test the display_check_string with missing columns in sr_output_need. Must exit the program.
    context = {
        'sr_output_need': {},
    }
    assertExit(lambda: exe_main.display_check_string(context))
    
def test_exe_main_dropbox_failure():
    
    # this test the exe_main with dropbox connection failing. Must exit the program.
    with patch("exe_main.dropbox_initiate_folder", side_effect=OSError("dropbox fail")):
        assertExit(lambda: exe_main.exe_main())
    
def test_exe_main_generate_output_need_failure():
    
    # this test the exe_main with generate_output_need failing. Must exit the program.
    mock_initiate_local_dict = {
         "df_paths": pd.read_csv("materials/paths.csv"),
         "sr_output_need" : pd.read_csv("materials/output_need_calculate.csv").iloc[0],
         'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
         'df_task_done' : pd.read_csv("materials/task_done.csv")
    }

    with patch("exe_main.dropbox_initiate_folder"), \
         patch("exe_main.fileA.initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch("exe_main.generate_output_need", side_effect=Exception("gen fail")):
        
        assertExit(lambda: exe_main.exe_main())

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_process_games_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_process_games_gameA_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_process_messages_extract_messages_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_process_messages_invalid_message_dataframe))
    test_suite.addTest(unittest.FunctionTestCase(test_display_check_string_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_exe_main_dropbox_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_exe_main_generate_output_need_failure))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
