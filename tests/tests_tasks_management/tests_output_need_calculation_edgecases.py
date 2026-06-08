'''
This tests file concern all functions in the output_need_calculation module.
It units test unhappy paths for each function
'''
import os
from unittest.mock import patch
from pandas.testing import assert_series_equal
import pandas as pd

from src.predict_core.tasks_management import output_need_calculation

def test_calculate_output_need_auto_empty_calendar(read_yml_as_serie, read_csv, assert_exit):
    
    # this test the function calculate_output_need_auto with an empty calendar. Must exit the program
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    df_task_done = read_csv("task_done.csv")
    str_current_run_time_utc = "2024-01-02 10:00:00.000"
    mock_df_calendar = read_csv("edgecases/calendar_empty.csv")

    with patch.object(output_need_calculation.tasks_calendar_management,"get_calendar", return_value=mock_df_calendar), \
         patch.object(output_need_calculation.tasks_calendar_management,"get_notrun_task", return_value=mock_df_calendar):

        assert_exit(lambda: output_need_calculation.calculate_output_need_auto(sr_snowflake_account_connect, df_task_done, str_current_run_time_utc))
      

def test_set_output_need_to_check_status(read_csv):
    
    # this test the function set_output_need_to_check_status
    sr_output_need = read_csv("output_need_calculate.csv").iloc[0]
    expected_sr = read_csv("output_need_check_without_optional_values.csv").iloc[0]
    with patch.object(output_need_calculation.var,'TMPF', '/fake/tmp'), \
         patch.object(output_need_calculation,"create_csv"):

        result = output_need_calculation.set_output_need_to_check_status(sr_output_need)
        result['TS_TASK_UTC'] = pd.to_datetime(result['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(result['TS_TASK_UTC'], pd.Timestamp) else result['TS_TASK_UTC']

        expected_sr['TS_TASK_UTC'] = pd.to_datetime(expected_sr['TS_TASK_UTC']).tz_localize(None) \
        if isinstance(expected_sr['TS_TASK_UTC'], pd.Timestamp) else expected_sr['TS_TASK_UTC']
        assert_series_equal(result.reset_index(drop=True), expected_sr.reset_index(drop=True),check_dtype=False,check_names=False)
