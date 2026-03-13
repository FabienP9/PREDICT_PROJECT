'''
This tests file concern all functions in the generate_output_need module.
It units test unhappy paths for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_series_equal
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import output_need_actions
from testutils import assertExit

def test_create_output_need_auto_empty_calendar():
    
    # this test the function create_output_need_auto with an empty calendar. Must exit the program
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    df_task_done = pd.read_csv("materials/task_done.csv")
    str_current_run_time_utc = "2024-01-02 10:00:00.000"
    mock_df_calendar = pd.read_csv("materials/edgecases/calendar_empty.csv")

    with patch.object(output_need_actions.calendarA,"get_calendar", return_value=mock_df_calendar), \
         patch.object(output_need_actions.calendarA,"get_notrun_task", return_value=mock_df_calendar):

        assertExit(lambda: output_need_actions.create_output_need_auto(sr_snowflake_account, df_task_done, str_current_run_time_utc))
        
def test_generate_output_need_manual_path():
    
    # this test the function generate_output_need with a manual path
    context_dict = {
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        "df_task_done": pd.read_csv("materials/task_done.csv"),
        "str_current_run_time_utc": "2024-01-02 10:00:00.000",
        "df_message_check_ts": pd.read_csv("materials/message_check_ts.csv"),
        "df_output_need_manual" : pd.read_csv("materials/output_need_calculate.csv")
    }
    os.environ["IS_OUTPUT_AUTO"] = "0"

    with patch.object(output_need_actions, "create_csv", return_value=None):
        result = output_need_actions.generate_output_need(context_dict)
        assert "LAST_MESSAGE_CHECK_TS_UTC" in result.index

def test_set_output_need_to_check_status():
    
    # this test the function set_output_need_to_check_status
    sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]
    expected_sr = pd.read_csv("materials/output_need_check_without_optional_values.csv").iloc[0]
    with patch.object(output_need_actions.config,'TMPF', '/fake/tmp'), \
         patch.object(output_need_actions,"create_csv"):

        result = output_need_actions.set_output_need_to_check_status(sr_output_need)
        result['TS_TASK_UTC'] = pd.to_datetime(result['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(result['TS_TASK_UTC'], pd.Timestamp) else result['TS_TASK_UTC']

        expected_sr['TS_TASK_UTC'] = pd.to_datetime(expected_sr['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(expected_sr['TS_TASK_UTC'], pd.Timestamp) else expected_sr['TS_TASK_UTC']
        assert_series_equal(result.reset_index(drop=True), expected_sr.reset_index(drop=True),check_dtype=False,check_names=False)
          
if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_create_output_need_auto_empty_calendar))
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_need_manual_path))
    test_suite.addTest(unittest.FunctionTestCase(test_set_output_need_to_check_status))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
