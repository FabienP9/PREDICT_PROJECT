'''
    The purpose of this module is to generate and interact 
    with the run main file "output_need" and its dataframe
'''
import logging
logging.basicConfig(level=logging.INFO)
import os
import pandas as pd
pd.set_option('display.max_rows', None) 
pd.set_option('display.max_columns', None) 
pd.set_option('display.width', None) 
pd.set_option('display.max_colwidth', None) 

import config
from file_actions.file_actions import create_csv
import calendar_actions as calendarA

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('str_current_run_time_utc',) })
def create_output_need_auto(sr_snowflake_account: pd.Series, df_task_done: pd.DataFrame, str_current_run_time_utc: str) -> pd.DataFrame | None :

    """
        Finds the output_need parameters if we calculate automatically
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameters to get the calendar
            df_task_done (dataframe) : Contains all task already run by the program
            str_current_run_time_utc (str) : The string which represents the current run timestamp
        Returns:
            Dataframe containing the output_need parameters, max 1 row. 
            If no appropriate row is found, returns None
        Raises:
            Exits the program if error running the function or if no output need is found (using decorator)
    """   
    
    logging.info("OUTPUTNEED -> CREATING OUTPUT_NEED - AUTOMATIC RUN [START]")
    # We get the calendar of run
    df_calendar = calendarA.get_calendar(sr_snowflake_account)
    [['TASK_RUN','SEASON_ID','GAMEDAY','TS_TASK_UTC','MESSAGE_ACTION','GAME_ACTION',
      'IS_TO_INIT','IS_TO_CALCULATE','IS_TO_RECALCULATE','IS_TO_DELETE']]

    # We first have to make sure we deal with timestamp and not strings
    df_calendar['TS_TASK_UTC'] = pd.to_datetime(df_calendar['TS_TASK_UTC'], errors='coerce')
    df_task_done['TS_TASK_UTC'] = pd.to_datetime(df_task_done['TS_TASK_UTC'], errors='coerce')
    ts_current_run = pd.to_datetime(str_current_run_time_utc, errors='coerce')

    #We are looking for actions at the current run timestamp so we filter both dataframe
    df_calendar_filtered = df_calendar[df_calendar['TS_TASK_UTC'] == ts_current_run]
    df_task_done_filtered = df_task_done[df_task_done['TS_TASK_UTC'] == ts_current_run]

    #We merge calendar and task_done to get what's not run yet
    df_notrun = calendarA.get_notrun_task(df_calendar_filtered,df_task_done_filtered)

    if not df_notrun.empty:
        #If there are several needs same utc time, we take alphabetically the first one - the other one will be run at next run
        df_output_need = df_notrun.sort_values(by=['TASK_RUN','SEASON_ID','GAMEDAY','TS_TASK_UTC'])
        logging.info("OUTPUTNEED -> CREATING OUTPUT_NEED - AUTOMATIC RUN [DONE]")
        return df_output_need
    
    else:
        raise ValueError(f"OUTPUTNEED -> Not found a matching output need")

@config.exit_program(log_filter=lambda args: {})
def generate_output_need(context_dict: dict) -> pd.Series:

    """
        Generates the file output_need, 
        which will parametrize what is needed to be run in the main function
        Args:
            context_dict (data dictionary) : Contains all python object needed for to generate the file
        Returns:
            series corresponding to the output_need file
        Raises:
            Exits the program if error running the function (using decorator)
    """

    logging.info("OUTPUTNEED -> GENERATING OUTPUT_NEED [START]")
    #if we don't want the output automatic, output will get from manual file
    if os.getenv("IS_OUTPUT_AUTO") == "0":
        df_output_need = context_dict['df_output_need_manual']

    #else, we calculate automatically using the current run timestamp and the task_done
    else:
        df_output_need = create_output_need_auto(context_dict['sr_snowflake_account_connect'],
                                                 context_dict['df_task_done'],
                                                 context_dict['str_current_run_time_utc'])

    #We add the last check_ts timestamp value to the output_need created

    df_output_need = pd.merge(df_output_need,context_dict['df_message_check_ts'], 
                                        on='SEASON_ID', 
                                        how='inner')

    df_output_need = df_output_need.rename(columns={'LAST_CHECK_TS_UTC': 'LAST_MESSAGE_CHECK_TS_UTC'})
    #Then we create the csv file output_need
    create_csv(os.path.join(config.TMPF,"output_need.csv"),df_output_need, config.need_encapsulated) 
    sr_output_need = df_output_need.iloc[0]
    
    logging.info("__________________________________________________________________")
    logging.info("IS RUNNING ==> ")
    logging.info(sr_output_need)
    logging.info("__________________________________________________________________")
    logging.info("OUTPUTNEED -> GENERATING OUTPUT_NEED [DONE]")
    return sr_output_need

@config.exit_program(log_filter=lambda args: {})
def set_output_need_to_check_status(sr_output_need: pd.Series) -> pd.Series:

    """
        Updates the output_need parameters (in place) to check status for MESSAGE_ACTION.
        Args:
            sr_output_need (series - one row) : Contains the serie of output_need
        Returns:
            series, one row, containing the new output_need parameters
        Raises:
            Exits the program if error running the function (using decorator)
    """
    
    logging.info("OUTPUTNEED -> UPDATING OUTPUT_NEED [START]")
    output_need_path = os.path.join(config.TMPF, 'output_need.csv')
    
    sr_output_need['TASK_RUN'] = config.TASK_RUN_MAP["CHECK"]
    sr_output_need['MESSAGE_ACTION'] = config.MESSAGE_ACTION_MAP["CHECK"]
    sr_output_need['GAME_ACTION'] = config.GAME_ACTION_MAP["AVOID"]
    sr_output_need['IS_TO_CALCULATE'] = 0
    sr_output_need['IS_TO_DELETE'] = 0
    sr_output_need['IS_TO_RECALCULATE'] = 0

    create_csv(output_need_path,sr_output_need.to_frame().T, config.need_encapsulated) 
    logging.info("OUTPUTNEED -> UPDATING OUTPUT_NEED [END]")    
    logging.info("__________________________________________________________________")
    logging.info("IS RUNNING UPDATED ==> TASK_RUN = CHECK")
    logging.info("__________________________________________________________________")
    
    return sr_output_need