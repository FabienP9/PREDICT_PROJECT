'''
This tests file concern all functions in the output_need_calculation module.
It units test the happy path for each function
'''

import os
from unittest.mock import patch
from pandas.testing import assert_series_equal
import pandas as pd

from src.predict_core.tasks_management import output_need_calculation

def test_calculate_output_need_auto(read_yml_as_serie, read_csv):
    
    # this test the function test_calculate_output_need_auto
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    df_task_done = read_csv("task_done.csv")
    str_current_run_time_utc = "2024-01-02 10:00:00.000"
    mock_df_calendar = read_csv("calendar.csv")
    expected_sr = read_csv("output_need_check.csv").iloc[0]

    with patch.object(output_need_calculation.tasks_calendar_management,"get_calendar", return_value=mock_df_calendar), \
         patch.object(output_need_calculation.tasks_calendar_management,"get_notrun_task", return_value=mock_df_calendar.iloc[[1]]):

        result = output_need_calculation.calculate_output_need_auto(
            sr_snowflake_account_connect, df_task_done, str_current_run_time_utc
        )

    assert_series_equal(result,expected_sr, check_names=False)

def test_generate_output_need_auto_path(read_yml_as_serie, read_csv):
    
    # this test the function generate_output_need with auto output_need and fake inputs
    context_dict = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        "df_task_done": read_csv("task_done.csv"),
        "str_current_run_time_utc": "2024-01-02 10:00:00.000",
        "df_message_check_ts": read_csv("message_check_ts.csv"),
    }
    os.environ["IS_OUTPUT_AUTO"] = "1"
    mock_sr_output_need = read_csv("output_need_check.csv").iloc[0]
    expected_sr = read_csv("output_need_check_with_message_check_ts.csv").iloc[0]

    with patch.object(output_need_calculation,"calculate_output_need_auto", return_value=mock_sr_output_need), \
         patch.object(output_need_calculation,"create_csv"):

        result = output_need_calculation.generate_output_need(context_dict)
        
        assert_series_equal(result.reset_index(drop=True), expected_sr.reset_index(drop=True),check_dtype=False,check_names=False)

def test_set_output_need_to_check_status_updates_series(read_csv):
   
    # this test the function set_output_need_to_check_status
    sr_output_need = read_csv("output_need_calculate.csv").iloc[0]
    expected_sr = read_csv("output_need_check_without_optional_values.csv").iloc[0]
    with patch.object(output_need_calculation.var,'TMPF', '/fake/tmp'), \
         patch.object(output_need_calculation,'create_csv'):

        result = output_need_calculation.set_output_need_to_check_status(sr_output_need)
        result['TS_TASK_UTC'] = pd.to_datetime(result['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(result['TS_TASK_UTC'], pd.Timestamp) else result['TS_TASK_UTC']

        expected_sr['TS_TASK_UTC'] = pd.to_datetime(expected_sr['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(expected_sr['TS_TASK_UTC'], pd.Timestamp) else expected_sr['TS_TASK_UTC']
        assert_series_equal(result.reset_index(drop=True), expected_sr.reset_index(drop=True),check_dtype=False,check_names=False)
