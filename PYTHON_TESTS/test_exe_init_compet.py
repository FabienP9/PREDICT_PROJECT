'''
This tests file concern all functions in the exe_init_compet module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, mock_open
import pandas as pd
import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import exe_init_compet

def test_exe_init_compet():
    
    # this test the function exe_init_compet mocking all dependencies
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "df_competition": pd.read_csv("materials/competition.csv"),
        "df_paths": pd.read_csv("materials/paths.csv"),
        "df_task_done": pd.read_csv("materials/task_done.csv")
    }
    mock_df_game = pd.read_csv("materials/game.csv")
    mock_str_next_run_time_utc = "2024-01-02 08:05:00.000"
    m = mock_open()

    with patch.object(exe_init_compet,"dropbox_initiate_folder"), \
         patch.object(exe_init_compet.fileA,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(exe_init_compet.gameA,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(exe_init_compet.fileA,"filter_data"), \
         patch.object(exe_init_compet.snowflakeA,"delete_tables_data_from_python"), \
         patch.object(exe_init_compet.snowflakeA,"update_snowflake"), \
         patch.object(exe_init_compet,"update_calendar_related_files", return_value=mock_str_next_run_time_utc), \
         patch.object(exe_init_compet.fileA,"terminate_local_environment"), \
         patch("builtins.open", m):

        exe_init_compet.exe_init_compet()

        m.assert_called_once_with("exe_output.json", "w")
        handle = m()
        written_data = "".join(call.args[0] for call in handle.write.call_args_list)
        result = json.loads(written_data)
        assert result == {"next_run": mock_str_next_run_time_utc}


if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_exe_init_compet))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
