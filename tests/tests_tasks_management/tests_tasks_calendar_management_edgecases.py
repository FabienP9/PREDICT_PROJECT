'''
This tests file concern all functions in the tasks_calendar_management module.
It units test unexpected paths
'''
import pandas as pd
from unittest.mock import patch

from src.predict_core.tasks_management import tasks_calendar_management

def test_get_notrun_task_malformed_timestamps(read_csv,assert_exit):
    
    # this test the function get_notrun_task with badly written timestamps. Must exit the program
    df_calendar = read_csv( "edgecases/calendar_with_bad_timestamp.csv")
    df_task_done = read_csv( "edgecases/task_done_with_bad_timestamp.csv")
    assert_exit(lambda: tasks_calendar_management.get_notrun_task(df_calendar, df_task_done))

def test_update_nextrun_all_null_timestamps(read_csv):
    
    # this test the function update_nextrun with null timestamps. Next run must be null
    df_calendar = read_csv( "calendar.csv")
    df_calendar['TS_TASK_UTC'] = pd.NaT
    df_task_done = df_calendar

    with patch.object(tasks_calendar_management.files_manipulation,'create_txt') as mock_create:
        result = tasks_calendar_management.update_nextrun(df_calendar, df_task_done)
        assert result == "NONE"
        mock_create.assert_called_once()
        assert "next_run_time_utc.txt" in mock_create.call_args[0][0]

def test_add_task_to_taskdone_missing_column(read_csv,assert_exit):
    
    # this test the function add_task_to_taskdone with columns non defined. Must exit the program
    sr_output_need = pd.DataFrame({'TASK_RUN': [1]}).iloc[0]  # Missing SEASON_ID, GAMEDAY, TS_TASK_UTC...
    df_task_done = read_csv( "task_done.csv")
    assert_exit(lambda: tasks_calendar_management.add_task_to_taskdone(sr_output_need, df_task_done))
