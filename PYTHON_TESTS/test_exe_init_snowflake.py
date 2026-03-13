'''
This tests file concern all functions in the exe_init_snowflake module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import exe_init_snowflake

def test_exe_init_snowflake_happy_path():
    
    # this test the function exe_init_snowflake mocking all dependencies
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "str_script_creating_database" : "", \
        "df_paths": pd.read_csv("materials/paths.csv"),

        "df_competition": pd.read_csv("materials/competition.csv"),
        "df_task_done": pd.read_csv("materials/task_done.csv")
    }

    with patch.object(exe_init_snowflake.config,"check_environment_variable"),\
         patch.object(exe_init_snowflake.dropboxA,"initiate_folder"), \
         patch.object(exe_init_snowflake.fileA,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(exe_init_snowflake.snowflakeA,"snowflake_execute_script"), \
         patch.object(exe_init_snowflake.dropboxA,"download_folder"), \
         patch.object(exe_init_snowflake.snowflakeA,"update_snowflake"), \
         patch.object(exe_init_snowflake.fileA,"terminate_local_environment"):

          exe_init_snowflake.exe_init_snowflake()

    
if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_exe_init_snowflake_happy_path))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
