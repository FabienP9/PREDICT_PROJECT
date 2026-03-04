'''
This tests file concern all functions in the output_need_actions module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import output_need_actions

def test_create_output_need_auto_returns_expected():
    
    # this test the function create_output_need_auto
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    df_task_done = pd.read_csv("materials/task_done.csv")
    str_current_run_time_utc = "2024-01-02 10:00:00.000"
    mock_df_calendar = pd.read_csv("materials/calendar.csv")
    expected_df = pd.read_csv("materials/output_need_check.csv")

    with patch.object(output_need_actions.calendarA,"get_calendar", return_value=mock_df_calendar), \
         patch.object(output_need_actions.calendarA,"get_notrun_task", return_value=mock_df_calendar.iloc[[1]]):

        result = output_need_actions.create_output_need_auto(
            sr_snowflake_account, df_task_done, str_current_run_time_utc
        )
        
        result['TS_TASK_UTC'] = pd.to_datetime(result['TS_TASK_UTC']).dt.tz_localize(None)
        expected_df['TS_TASK_UTC'] = pd.to_datetime(expected_df['TS_TASK_UTC']).dt.tz_localize(None)

    assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True),check_dtype=False)

def test_generate_output_need_auto_path():
    
    # this test the function generate_output_need with auto output_need and fake inputs
    context_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "df_task_done": pd.read_csv("materials/task_done.csv"),
        "str_current_run_time_utc": "2024-01-02 10:00:00.000",
        "df_message_check_ts": pd.read_csv("materials/message_check_ts.csv"),
    }
    os.environ["IS_OUTPUT_AUTO"] = "1"
    mock_df_output_need = pd.read_csv("materials/output_need_check.csv")
    expected_sr = pd.read_csv("materials/output_need_check_with_message_check_ts.csv").iloc[0]

    with patch.object(output_need_actions,"create_output_need_auto", return_value=mock_df_output_need), \
         patch.object(output_need_actions,"create_csv"):

        result = output_need_actions.generate_output_need(context_dict)
        result['TS_TASK_UTC'] = pd.to_datetime(result['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(result['TS_TASK_UTC'], pd.Timestamp) else result['TS_TASK_UTC']

        expected_sr['TS_TASK_UTC'] = pd.to_datetime(expected_sr['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(expected_sr['TS_TASK_UTC'], pd.Timestamp) else expected_sr['TS_TASK_UTC']
        
        assert_series_equal(result.reset_index(drop=True), expected_sr.reset_index(drop=True),check_dtype=False,check_names=False)

def test_set_output_need_to_check_status_updates_series():
   
    # this test the function set_output_need_to_check_status
    sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]
    expected_sr = pd.read_csv("materials/output_need_check_without_optional_values.csv").iloc[0]
    with patch.object(output_need_actions.config,'TMPF', '/fake/tmp'), \
         patch.object(output_need_actions,'create_csv'):

        result = output_need_actions.set_output_need_to_check_status(sr_output_need)
        result['TS_TASK_UTC'] = pd.to_datetime(result['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(result['TS_TASK_UTC'], pd.Timestamp) else result['TS_TASK_UTC']

        expected_sr['TS_TASK_UTC'] = pd.to_datetime(expected_sr['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(expected_sr['TS_TASK_UTC'], pd.Timestamp) else expected_sr['TS_TASK_UTC']
        assert_series_equal(result.reset_index(drop=True), expected_sr.reset_index(drop=True),check_dtype=False,check_names=False)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_create_output_need_auto_returns_expected))
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_need_auto_path))
    test_suite.addTest(unittest.FunctionTestCase(test_set_output_need_to_check_status_updates_series))
    
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
