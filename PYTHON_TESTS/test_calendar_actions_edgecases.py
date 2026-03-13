'''
This tests file concern all functions in the calendar_actions module.
It units test unexpected paths for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import calendar_actions
from testutils import assertExit

def test_get_calendar_snowflake_execute_fails():
    
    # this test the function get_calendar with failed snowflake execution. Must exit the program
    sr_snowflake_account_connect = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    assertExit(lambda: calendar_actions.get_calendar(sr_snowflake_account_connect))

def test_get_notrun_task_malformed_timestamps():
    
    # this test the function get_notrun_task with badly written timestamps. Must exit the program
    df_calendar = pd.read_csv("materials/edgecases/calendar_with_bad_timestamp.csv")
    df_task_done = pd.read_csv("materials/edgecases/task_done_with_bad_timestamp.csv")
    assertExit(lambda: calendar_actions.get_notrun_task(df_calendar, df_task_done))

def test_update_nextrun_all_null_timestamps():
    
    # this test the function update_nextrun with null timestamps. Next run must be null
    df_calendar = pd.read_csv("materials/calendar.csv")
    df_calendar['TS_TASK_UTC'] = pd.NaT
    df_task_done = df_calendar

    with patch.object(calendar_actions.fileA,'create_txt') as mock_create:
        result = calendar_actions.update_nextrun(df_calendar, df_task_done)
        assert result == "NONE"
        mock_create.assert_called_once()
        assert "next_run_time_utc.txt" in mock_create.call_args[0][0]

def test_add_task_to_taskdone_missing_column():
    
    # this test the function add_task_to_taskdone with columns non defined. Must exit the program
    sr_output_need = pd.DataFrame({'TASK_RUN': [1]}).iloc[0]  # Missing SEASON_ID, GAMEDAY, TS_TASK_UTC...
    df_task_done = pd.read_csv("materials/task_done.csv")
    assertExit(lambda: calendar_actions.add_task_to_taskdone(sr_output_need, df_task_done))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_calendar_snowflake_execute_fails))
    test_suite.addTest(unittest.FunctionTestCase(test_get_notrun_task_malformed_timestamps))
    test_suite.addTest(unittest.FunctionTestCase(test_update_nextrun_all_null_timestamps))
    test_suite.addTest(unittest.FunctionTestCase(test_add_task_to_taskdone_missing_column))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)