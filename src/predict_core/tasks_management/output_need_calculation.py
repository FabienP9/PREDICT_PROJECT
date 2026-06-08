'''
    The purpose of this module is to generate and interact 
    with the run main file "output_need" and its dataframe
'''
import logging
import os
import pandas as pd

from ..config import config_decorators
from ..config.config_variables import config_global_variables as var
from . import tasks_calendar_management
from ..files_manipulation.local_files_manipulation.files_manipulation import create_csv

logging.basicConfig(level=logging.INFO)
pd.set_option('display.max_rows', None) 
pd.set_option('display.max_columns', None) 
pd.set_option('display.width', None) 
pd.set_option('display.max_colwidth', None) 

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_output_need_manual',) })
def calculate_output_need_from_manual(sr_snowflake_account: pd.Series, sr_output_need_manual: pd.Series) -> pd.Series:

    """
        Calculates the task to run if ran manually (IS_OUTPUT_AUTO = 0)
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameters to get the calendar
            sr_output_need_manual (series - one row) : Contains the desired output_need from output_need_manual file
        Returns:
            Series the output_need parameters, max 1 row. 
            If no appropriate row is found, raise an error
        Raises:
            Exits the program if error running the function or if no output need is found (using decorator)
    """   
    
    logging.info("OUTPUT NEED -> CALCULATING OUTPUT NEED - MANUAL RUN [START]")
    
    # We get the calendar of run
    df_calendar = tasks_calendar_management.get_calendar(sr_snowflake_account)
    df_calendar['TS_TASK_UTC'] = pd.to_datetime(df_calendar['TS_TASK_UTC'], errors='coerce')
    matching_rows = df_calendar[
        (df_calendar['TASK_RUN'] == sr_output_need_manual['TASK_RUN'].strip()) &
        (df_calendar['SEASON_ID'] == sr_output_need_manual['SEASON_ID'].strip()) &
        (df_calendar['GAMEDAY'] == sr_output_need_manual['GAMEDAY'].strip())
    ].copy()
    
    if len(matching_rows) == 0:
        raise ValueError ("Not found a matching output need in the calendar")

    # We extract the closest matching row from the current date
    current_date = pd.Timestamp.now()
    matching_rows['date_diff'] = abs((matching_rows['TS_TASK_UTC'] - current_date).dt.total_seconds())
    closest_row_idx = matching_rows['date_diff'].idxmin()

    sr_output_need = pd.Series({
        'TASK_RUN': df_calendar.loc[closest_row_idx, 'TASK_RUN'],
        'SEASON_ID': df_calendar.loc[closest_row_idx, 'SEASON_ID'],
        'SEASON_SPORT': df_calendar.loc[closest_row_idx, 'SEASON_SPORT'],
        'SEASON_COUNTRY': df_calendar.loc[closest_row_idx, 'SEASON_COUNTRY'],
        'SEASON_NAME': df_calendar.loc[closest_row_idx, 'SEASON_NAME'],
        'SEASON_DIVISION': df_calendar.loc[closest_row_idx, 'SEASON_DIVISION'],
        'COMPETITION_ID': df_calendar.loc[closest_row_idx, 'COMPETITION_ID'],
        'COMPETITION_SOURCE_ID': df_calendar.loc[closest_row_idx, 'COMPETITION_SOURCE_ID'],
        'GAMEDAY': df_calendar.loc[closest_row_idx, 'GAMEDAY'],
        'TS_TASK_UTC': df_calendar.loc[closest_row_idx, 'TS_TASK_UTC'],
        'TS_TASK_LOCAL': df_calendar.loc[closest_row_idx, 'TS_TASK_LOCAL'],
        'IS_TO_INIT': sr_output_need_manual['IS_TO_INIT'],
        'IS_TO_CALCULATE': sr_output_need_manual['IS_TO_CALCULATE'],
        'IS_TO_DELETE': sr_output_need_manual['IS_TO_DELETE'],
        'IS_TO_RECALCULATE': sr_output_need_manual['IS_TO_RECALCULATE'],
        'MESSAGE_ACTION': sr_output_need_manual['MESSAGE_ACTION'],
        'GAME_ACTION': sr_output_need_manual['GAME_ACTION']
    })

    logging.info("OUTPUT NEED -> CALCULATING OUTPUT NEED - MANUAL RUN [DONE]")
    return sr_output_need

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('str_current_run_time_utc',) })
def calculate_output_need_auto(sr_snowflake_account: pd.Series, df_task_done: pd.DataFrame, str_current_run_time_utc: str) -> pd.Series :

    """
        Calculates the task to run if ran automatically (IS_OUTPUT_AUTO = 1)
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameters to get the calendar
            df_task_done (dataframe) : Contains all task already run by the program
            str_current_run_time_utc (str) : The string which represents the current run timestamp
        Returns:
            Serie containing the output_need parameters. 
            If no appropriate row is found, raise an error
        Raises:
            Exits the program if error running the function or if no output need is found (using decorator)
    """   
    
    logging.info("OUTPUT NEED -> CALCULATING OUTPUT NEED - AUTOMATIC RUN [START]")
    # We get the calendar of run
    df_calendar = tasks_calendar_management.get_calendar(sr_snowflake_account)

    # We first have to make sure we deal with timestamp and not strings
    df_calendar['TS_TASK_UTC'] = pd.to_datetime(df_calendar['TS_TASK_UTC'], errors='coerce')
    df_task_done['TS_TASK_UTC'] = pd.to_datetime(df_task_done['TS_TASK_UTC'], errors='coerce')
    ts_current_run = pd.to_datetime(str_current_run_time_utc, errors='coerce')

    #We are looking for actions at the current run timestamp so we filter both dataframe
    df_calendar_filtered = df_calendar[df_calendar['TS_TASK_UTC'] == ts_current_run]
    df_task_done_filtered = df_task_done[df_task_done['TS_TASK_UTC'] == ts_current_run]

    #We merge calendar and task_done to get what's not run yet
    df_notrun = tasks_calendar_management.get_notrun_task(df_calendar_filtered,df_task_done_filtered)

    if not df_notrun.empty:
        #If there are several needs same utc time, we take alphabetically the first one - the other one will be run at next run
        df_output_need = df_notrun.sort_values(by=['TASK_RUN','SEASON_ID','GAMEDAY','TS_TASK_UTC'])
        logging.info("OUTPUT NEED -> CALCULATING OUTPUT NEED - AUTOMATIC RUN [DONE]")
        return df_output_need.iloc[0]
    
    raise ValueError("OUTPUT NEED -> Not found a matching task")

@config_decorators.exit_program(log_filter=lambda args: {})
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
    logging.info("OUTPUT NEED -> GENERATING OUTPUT NEED [START]")
    #if we don't want the output automatic, output will get from manual file
    if os.getenv("IS_OUTPUT_AUTO") == "0":
        sr_output_need = calculate_output_need_from_manual(context_dict['sr_snowflake_account_connect'],
                                                           context_dict['sr_output_need_manual'])
                                                           
    #else, we calculate automatically using the current run timestamp and the task_done
    else:
        sr_output_need = calculate_output_need_auto(context_dict['sr_snowflake_account_connect'],
                                                 context_dict['df_task_done'],
                                                 context_dict['str_current_run_time_utc'])

    # We add the last check_ts timestamp value to the output_need created
    sr_output_need['LAST_MESSAGE_CHECK_TS_UTC'] = context_dict['df_message_check_ts'].loc[context_dict['df_message_check_ts']['SEASON_ID'] == 
                                                                                          sr_output_need['SEASON_ID'], 'LAST_CHECK_TS_UTC'].iloc[0]
    
    #Then we create the csv file output_need
    create_csv(os.path.join(var.TMPF,"output_need.csv"),sr_output_need.to_frame().T) 
    
    logging.info("__________________________________________________________________")
    logging.info("IS RUNNING ==> ")
    logging.info(sr_output_need)
    logging.info("__________________________________________________________________")
    logging.info("OUTPUT NEED -> GENERATING OUTPUT NEED [DONE]")
    return sr_output_need

@config_decorators.exit_program(log_filter=lambda args: {})
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
    
    logging.info("OUTPUT NEED -> UPDATING OUTPUT NEED [START]")
    output_need_path = os.path.join(var.TMPF, 'output_need.csv')
    
    sr_output_need['TASK_RUN'] = var.TASK_RUN_MAP["CHECK"]
    sr_output_need['MESSAGE_ACTION'] = var.MESSAGE_ACTION_MAP["CHECK"]
    sr_output_need['GAME_ACTION'] = var.GAME_ACTION_MAP["AVOID"]
    sr_output_need['IS_TO_CALCULATE'] = 0
    sr_output_need['IS_TO_DELETE'] = 0
    sr_output_need['IS_TO_RECALCULATE'] = 0

    create_csv(output_need_path,sr_output_need.to_frame().T, var.OUTPUT_NEED_ENCAPSULATED) 
    logging.info("OUTPUT NEED -> UPDATING OUTPUT_NEED [END]")    
    logging.info("__________________________________________________________________")
    logging.info("IS RUNNING UPDATED ==> OUTPUT NEED = CHECK")
    logging.info("__________________________________________________________________")
    
    return sr_output_need