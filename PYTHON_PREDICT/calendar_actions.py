'''
    The purpose of this module is to interact with calendar:
    - get it from Snowflake view
    - get the task already done, and add some
    - get task not already done to calculate with task must be run
'''

import logging
logging.basicConfig(level=logging.INFO)
import os
import pandas as pd

from file_actions import file_actions as fileA
import config
from snowflake_actions import snowflake_execute
import sql_queries as sqlQ

@config.exit_program(log_filter=lambda args: {})
def get_calendar(sr_snowflake_account: pd.Series) -> pd.DataFrame:

        """
        Get the calendar of program run
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameters to run a query
        Returns:
            dataframe containing the result of the query which represents the calendar
        Raises:
            Exits the program if error running the function (using decorator)
        """

        logging.info("SNOWFLAKE -> GETTING RUN CALENDAR [START]")
        #The calendar is stored on a snowflake view
        df_calendar = snowflake_execute(sr_snowflake_account,sqlQ.calendar_actions_qCalendar)

        logging.info("SNOWFLAKE -> GETTING RUN CALENDAR [DONE]")
        return df_calendar

@config.exit_program(log_filter=lambda args: {'columns_df_calendar': args['df_calendar'].columns.tolist(), 'columns_df_task_done': args['df_task_done'].columns.tolist() })
def get_notrun_task(df_calendar: pd.DataFrame,df_task_done: pd.DataFrame) -> pd.DataFrame:
     
    """
    Gets which task from the calendar have not yet been run
    Args:
        df_calendar (dataframe) : Contains the updated calendar of action, including their timestamp
        df_task_done (dataframe) : Contains task already run
    Return:
        The dataframe of task not yet run
    Raises:
        Exits the program if error running the function (using decorator)
    """

    # we make sure timestamp are well formatted
    df_calendar['TS_TASK_UTC'] = pd.to_datetime(df_calendar['TS_TASK_UTC'])
    df_task_done['TS_TASK_UTC'] = pd.to_datetime(df_task_done['TS_TASK_UTC'])

    # We remove from df_calendar what have already been run
    merge = df_calendar.merge(df_task_done.drop_duplicates(), 
                              how='left', 
                              on=['TASK_RUN','SEASON_ID','GAMEDAY','TS_TASK_UTC'], 
                              indicator=True)
    
    df_notrun = merge[merge['_merge'] == 'left_only'].drop(columns=['_merge'])
    return df_notrun

@config.exit_program(log_filter=lambda args: {})
def update_nextrun(df_calendar: pd.DataFrame, df_task_done: pd.DataFrame) -> str:

    """
    Updates next schedule run timestamp after consumption of the current one, 
            by comparing the two dataframe in input and creates the related txt file after update
    Args:
        df_calendar (dataframe) : Contains the updated calendar of action, including their timestamp
        df_task_done (dataframe) : Contains task already done
    Return:
        The string of the next schedule run (YYYY-MM-DD HH:mm:ss.000) or NONE if there is not any
    Raises:
        Exits the program if error running the function (using decorator)
    """

    logging.info("CALENDAR -> UPDATE NEXT RUN [START]")

    #We first get the part of the calendar which has not been run yet
    df_notrun = get_notrun_task(df_calendar,df_task_done)    

    if df_notrun.empty or df_notrun['TS_TASK_UTC'].isnull().all():
        str_next_run_utc = "NONE"
    else:
        ts_next_run_utc = df_notrun['TS_TASK_UTC'].min()
        str_next_run_utc = str(ts_next_run_utc.strftime("%Y-%m-%d %H:%M:%S.000"))

    fileA.create_txt(os.path.join(config.TMPF,'next_run_time_utc.txt'),str_next_run_utc)
    logging.info("__________________________________________________________________")
    logging.info("NEXT RUN TIME ==> ")
    logging.info(str_next_run_utc)
    logging.info("__________________________________________________________________")
    logging.info("CALENDAR -> UPDATE NEXT RUN [DONE]")
    return str_next_run_utc

@config.exit_program(log_filter=lambda args: {})
def add_task_to_taskdone(sr_output_need: pd.Series, df_task_done: pd.DataFrame) -> pd.DataFrame:

    """
    Adds the task to the list of task done and creates the csv related file
    Args:
        sr_output_need (series - one row) : The output_need serie. We add it to task done
        df_task_done (dataframe) : Contains task already done
    Return:
        The updated task done dataframe
    Raises:
        Exits the program if error running the function (using decorator)
    """

    row = pd.DataFrame({'TASK_RUN': [sr_output_need['TASK_RUN']], 
                        'SEASON_ID': [sr_output_need['SEASON_ID']], 
                        'GAMEDAY': [sr_output_need['GAMEDAY']], 
                        'TS_TASK_UTC': [sr_output_need['TS_TASK_UTC']]})
    df_task_done_modified = pd.concat([row,df_task_done], ignore_index=True)
    fileA.create_csv(os.path.join(config.TMPF,"task_done.csv"),df_task_done_modified,is_to_encapsulate=config.task_done_encapsulated)

    return df_task_done_modified

@config.exit_program(log_filter=lambda args: {})
def update_calendar_related_files(called_by: str, sr_snowflake_account: pd.Series, df_task_done: pd.DataFrame, sr_output_need: pd.Series | None = None):
    
    """
        The purpose of this function is update calendar files (task_done and next_run_ts)
        Args:
            called_by (string): The "exe" function calling this one, task_done will be updated only with main
            sr_snowflake_account (series - one row) : Contains the snowflake account parameters to run a query
            df_task_done (dataframe) : Contains task already done
            sr_output_need (series - one row) : The output_need serie. We add it to task done when provided (for main execution)
        Return:
            The next run timestamp updated
        Raises:
            Exits the program if error running the function (using decorator)
    """

    if called_by == config.CALLER["MAIN"]:
        # we add the run to the task done 
        df_task_done = add_task_to_taskdone(sr_output_need,df_task_done)
    
    # the calendar might have change on the database - we get the updated calendar
    df_calendar = get_calendar(sr_snowflake_account)[['TASK_RUN','SEASON_ID','GAMEDAY','TS_TASK_UTC']]
    
    # we then update the next run accordingly by comparing the updated calendar with the task done possibly modified before
    str_next_run = update_nextrun(df_calendar,df_task_done)
    return str_next_run