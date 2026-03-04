'''
This tests file concern all functions in the exe_init_compet module.
It units test unhappy paths
'''

import unittest
from unittest.mock import patch
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import exe_init_compet
from testutils import assertExit

def test_dropbox_initiate_folder_failure():
    
    # this test the function exe_init_compet with a failing dropboxA.initiate_folder. Must exit program
    with patch.object(exe_init_compet,"dropbox_initiate_folder", side_effect=Exception("Dropbox error")):
        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_initiate_local_environment_missing_key():
    
    # this test the function exe_init_compet with a failing fileA.initiate_local_environment, because of missing keys. Must exit the program
    mock_initiate_local_dict = {}
    with patch.object(exe_init_compet,"dropbox_initiate_folder"), \
         patch.object(exe_init_compet.fileA,"initiate_local_environment", return_value = mock_initiate_local_dict):

        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_filter_data_failure():
    
    # this test the function exe_init_compet with a failing fileA.filter_data. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "df_competition": pd.read_csv("materials/competition.csv"),
        "df_paths": pd.read_csv("materials/paths.csv"),
        "df_task_done": pd.read_csv("materials/task_done.csv")
    }
    mock_df_game = pd.read_csv("materials/game.csv")
    
    with patch.object(exe_init_compet,"dropbox_initiate_folder"), \
         patch.object(exe_init_compet.fileA,"initiate_local_environment", mock_initiate_local_dict), \
         patch.object(exe_init_compet.gameA,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(exe_init_compet.fileA,"filter_data", side_effect=Exception("Filter failed")):

        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_snowflake_update_failure():
    
    # this test the function exe_init_compet with a failing snowflakeA.update_snowflake. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "df_competition": pd.read_csv("materials/competition.csv"),
        "df_paths": pd.read_csv("materials/paths.csv"),
        "df_task_done": pd.read_csv("materials/task_done.csv")
    }
    mock_df_game = pd.read_csv("materials/game.csv")

    with patch.object(exe_init_compet,"dropbox_initiate_folder"), \
         patch.object(exe_init_compet.fileA,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(exe_init_compet.gameA,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(exe_init_compet.fileA,"filter_data"), \
         patch.object(exe_init_compet.snowflakeA,"delete_tables_data_from_python"), \
         patch.object(exe_init_compet.snowflakeA,"update_snowflake"):

        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_calendar_update_failure():
    
    # this test the function exe_init_compet with a failing update_calendar_related_files. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "df_competition": pd.read_csv("materials/competition.csv"),
        "df_paths": pd.read_csv("materials/paths.csv"),
        "df_task_done": pd.read_csv("materials/task_done.csv")
    }
    mock_df_game = pd.read_csv("materials/game.csv")

    with patch.object(exe_init_compet,"dropbox_initiate_folder"), \
         patch.object(exe_init_compet.fileA,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(exe_init_compet.gameA,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(exe_init_compet.fileA,"filter_data"), \
         patch.object(exe_init_compet.snowflakeA,"delete_tables_data_from_python"), \
         patch.object(exe_init_compet.snowflakeA,"update_snowflake"), \
         patch.object(exe_init_compet,"update_calendar_related_files", side_effect=Exception("Calendar error")):
    
        assertExit(lambda: exe_init_compet.exe_init_compet())

def test_terminate_local_environment_failure():
    
    # this test the function exe_init_compet with a failing fileA.terminate_local_environment. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "df_competition": pd.read_csv("materials/competition.csv"),
        "df_paths": pd.read_csv("materials/paths.csv"),
        "df_task_done": pd.read_csv("materials/task_done.csv")
    }
    mock_df_game = pd.read_csv("materials/game.csv")
    mock_str_next_run_time_utc = "2024-01-02 08:05:00.000"

    with patch.object(exe_init_compet,"dropbox_initiate_folder"), \
         patch.object(exe_init_compet.fileA,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(exe_init_compet.gameA,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(exe_init_compet.fileA,"filter_data"), \
         patch.object(exe_init_compet.snowflakeA,"delete_tables_data_from_python"), \
         patch.object(exe_init_compet.snowflakeA,"update_snowflake"), \
         patch.object(exe_init_compet,"update_calendar_related_files", return_value=mock_str_next_run_time_utc), \
         patch.object(exe_init_compet.fileA,"terminate_local_environment", side_effect=Exception("Terminate failed")):

        assertExit(lambda: exe_init_compet.exe_init_compet())

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_dropbox_initiate_folder_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_initiate_local_environment_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_filter_data_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_snowflake_update_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_calendar_update_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_terminate_local_environment_failure))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)