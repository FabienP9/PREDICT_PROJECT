'''
This tests file concern all functions in the tasks_calendar_management module.
It units test the happy path for each function
'''
from unittest.mock import patch
from pandas.testing import assert_frame_equal
import pandas as pd

from src.predict_core.tasks_management import tasks_calendar_management

def test_get_calendar(read_yml_as_serie, read_csv):
    
    # this test the function get_calendar
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    mock_df_calendar = read_csv("calendar.csv")

    with patch.object(tasks_calendar_management,"snowflake_execute", return_value=mock_df_calendar):
        expected_df = mock_df_calendar
        result_df = tasks_calendar_management.get_calendar(sr_snowflake_account_connect)

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_get_notrun_task(read_csv):
    
    # this test the function get_notrun_task
    df_calendar = read_csv("calendar.csv")
    df_task_done = read_csv("task_done.csv")

    expected_df = df_calendar.iloc[[1]]
    expected_df['TS_TASK_UTC'] = pd.to_datetime(expected_df['TS_TASK_UTC'], errors='coerce')

    result_df = tasks_calendar_management.get_notrun_task(df_calendar.copy(), df_task_done.copy())
    assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_update_nextrun(read_csv):
    
    # this test the function update_nextrun
    df_calendar = read_csv("calendar.csv")
    df_task_done = read_csv("task_done.csv")
    mock_df_notrun = df_calendar.iloc[[1]]
    mock_df_notrun['TS_TASK_UTC'] = pd.to_datetime(mock_df_notrun['TS_TASK_UTC'], errors='coerce')

    with patch.object(tasks_calendar_management,"get_notrun_task", return_value= mock_df_notrun), \
         patch.object(tasks_calendar_management.files_manipulation,"create_txt") as mock_create_txt:
        
        result = tasks_calendar_management.update_nextrun(df_calendar, df_task_done)
        assert result == "2024-01-02 10:00:00.000"
        mock_create_txt.assert_called_once()

def test_add_task_to_taskdone(read_csv):
    
    # this test the function add_task_to_taskdone
    sr_output_need = read_csv("output_need_init.csv").iloc[0]
    df_task_done = read_csv("task_done.csv")
    expected_df = read_csv("task_done_after_add.csv")

    with patch.object(tasks_calendar_management.files_manipulation,"create_csv") as mock_create_csv:
        result = tasks_calendar_management.add_task_to_taskdone(sr_output_need, df_task_done)
        assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True))
        mock_create_csv.assert_called_once()

def test_update_calendar_related_files_main(read_yml_as_serie, read_csv):
    
    # this test the function update_calendar_related_files from caller main
    called_by = "main"
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    df_task_done = read_csv("task_done.csv")
    sr_output_need = read_csv("output_need_init.csv").iloc[0]
    mock_df_task_done = read_csv("task_done_after_add.csv")
    mock_df_calendar = read_csv("calendar.csv")
    mock_nextrun =  "2024-01-02 10:00:00.000"

    with patch.object(tasks_calendar_management,"add_task_to_taskdone", return_value=mock_df_task_done), \
         patch.object(tasks_calendar_management,"get_calendar", return_value=mock_df_calendar), \
         patch.object(tasks_calendar_management,"update_nextrun", return_value=mock_nextrun):
    
        result = tasks_calendar_management.update_calendar_related_files(called_by, sr_snowflake_account_connect, df_task_done, sr_output_need)
        assert result == "2024-01-02 10:00:00.000"

def test_update_calendar_related_files_compet(read_yml_as_serie, read_csv):
    
    # this test the function update_calendar_related_files when called by competition_integration
    called_by = "init_compet"
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    df_task_done = read_csv("task_done.csv")
    sr_output_need = read_csv("output_need_init.csv").iloc[0]
    mock_df_calendar = read_csv("calendar.csv")
    mock_nextrun =  "2024-01-02 10:00:00.000"

    with patch.object(tasks_calendar_management,"get_calendar", return_value=mock_df_calendar), \
         patch.object(tasks_calendar_management,"update_nextrun", return_value=mock_nextrun):
    
        result = tasks_calendar_management.update_calendar_related_files(called_by, sr_snowflake_account_connect, df_task_done, sr_output_need)
        assert result == "2024-01-02 10:00:00.000"
