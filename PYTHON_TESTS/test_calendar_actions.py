'''
This tests file concern all functions in the calendar_actions module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import calendar_actions

def test_get_calendar():
    
    # this test the function get_calendar
    sr_snowflake_account_connect = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    mock_df_calendar = pd.read_csv("materials/calendar.csv")

    with patch("calendar_actions.snowflake_execute", return_value=mock_df_calendar):
        expected_df = mock_df_calendar
        result_df = calendar_actions.get_calendar(sr_snowflake_account_connect)

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_get_notrun_task():
    
    # this test the function get_notrun_task
    df_calendar = pd.read_csv("materials/calendar.csv")
    df_task_done = pd.read_csv("materials/task_done.csv")

    expected_df = df_calendar.iloc[[1]]
    expected_df['TS_TASK_UTC'] = pd.to_datetime(expected_df['TS_TASK_UTC'], errors='coerce')

    result_df = calendar_actions.get_notrun_task(df_calendar.copy(), df_task_done.copy())
    assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_update_nextrun():
    
    # this test the function update_nextrun
    df_calendar = pd.read_csv("materials/calendar.csv")
    df_task_done = pd.read_csv("materials/task_done.csv")
    mock_df_notrun = df_calendar.iloc[[1]]
    mock_df_notrun['TS_TASK_UTC'] = pd.to_datetime(mock_df_notrun['TS_TASK_UTC'], errors='coerce')

    with patch.object(calendar_actions,"get_notrun_task", return_value= mock_df_notrun), \
         patch.object(calendar_actions.fileA,"create_txt") as mock_create_txt:
        
        result = calendar_actions.update_nextrun(df_calendar, df_task_done)
        assert result == "2024-01-02 10:00:00.000"
        mock_create_txt.assert_called_once()

def test_add_task_to_taskdone():
    
    # this test the function add_task_to_taskdone
    sr_output_need = pd.read_csv("materials/output_need_init.csv").iloc[0]
    df_task_done = pd.read_csv("materials/task_done.csv")
    expected_df = pd.read_csv("materials/task_done_after_add.csv")

    with patch.object(calendar_actions.fileA,"create_csv") as mock_create_csv:
        result = calendar_actions.add_task_to_taskdone(sr_output_need, df_task_done)
        assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True))
        mock_create_csv.assert_called_once()

def test_update_calendar_related_files_main():
    
    # this test the function update_calendar_related_files from caller exe_main
    called_by = "main"
    sr_snowflake_account_connect = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    df_task_done = pd.read_csv("materials/task_done.csv")
    sr_output_need = pd.read_csv("materials/output_need_init.csv").iloc[0]
    mock_df_task_done = pd.read_csv("materials/task_done_after_add.csv")
    mock_df_calendar = pd.read_csv("materials/calendar.csv")
    mock_nextrun =  "2024-01-02 10:00:00.000"

    with patch.object(calendar_actions,"add_task_to_taskdone", return_value=mock_df_task_done), \
         patch.object(calendar_actions,"get_calendar", return_value=mock_df_calendar), \
         patch.object(calendar_actions,"update_nextrun", return_value=mock_nextrun):
    
        result = calendar_actions.update_calendar_related_files(called_by, sr_snowflake_account_connect, df_task_done, sr_output_need)
        assert result == "2024-01-02 10:00:00.000"

def test_update_calendar_related_files_compet():
    
    # this test the function update_calendar_related_files when called by exe_init_compet
    called_by = "init_compet"
    sr_snowflake_account_connect = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    df_task_done = pd.read_csv("materials/task_done.csv")
    sr_output_need = pd.read_csv("materials/output_need_init.csv").iloc[0]
    mock_df_calendar = pd.read_csv("materials/calendar.csv")
    mock_nextrun =  "2024-01-02 10:00:00.000"

    with patch.object(calendar_actions,"get_calendar", return_value=mock_df_calendar), \
         patch.object(calendar_actions,"update_nextrun", return_value=mock_nextrun):
    
        result = calendar_actions.update_calendar_related_files(called_by, sr_snowflake_account_connect, df_task_done, sr_output_need)
        assert result == "2024-01-02 10:00:00.000"

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_calendar))
    test_suite.addTest(unittest.FunctionTestCase(test_get_notrun_task))
    test_suite.addTest(unittest.FunctionTestCase(test_update_nextrun))
    test_suite.addTest(unittest.FunctionTestCase(test_add_task_to_taskdone))
    test_suite.addTest(unittest.FunctionTestCase(test_update_calendar_related_files_main))
    test_suite.addTest(unittest.FunctionTestCase(test_update_calendar_related_files_compet))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)