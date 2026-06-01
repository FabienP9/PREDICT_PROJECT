'''
    The purpose of this module is to interact with specific local files
'''
import logging
import os
import pandas as pd
import ast
from datetime import datetime, timezone
from re import sub as re_sub
import json

from ....config import config_decorators
from ....config.config_variables import config_global_variables as var
from ....files_manipulation.external_files_interaction import dropbox_files_interaction as dropbox
from ....files_manipulation.local_files_manipulation import files_manipulation

logging.basicConfig(level=logging.INFO)

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def get_paths_file_details() -> dict:

    """
        The purpose of this function is to extract details from paths file to avoid hardcoding all paths here
        - downloads the file of paths from DropBox
        - manipulate it for having good type columns
        - create the dataframe related
        Returns:
            data dictionary containing the dataframe created
        Raises:
            Exits the program if error running the function (using decorator)
    """
    
    #We download the file which contains all file paths on DropBox, to avoid hardcoding every path here
    files_data_dict = dropbox.download_file(dropbox_file_path = var.PATHS_FILE,
                                            local_folder = var.TMPF,
                                            is_encapsulated=var.PATHS_FILE_ENCAPSULATED,
                                            is_path_abs=1)
    
    #We change the type of some of its columns to a list
    columns_to_convert = ["FILTERING_COLUMN", "DOWNLOAD_CATEGORY", "PYTHON_CATEGORY", "DBT_CATEGORY"]
    for col in columns_to_convert:
        files_data_dict['df_paths'][col] = files_data_dict['df_paths'][col].map(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else x
    )

    return files_data_dict

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def modify_run_type_file(df_run_type: pd.DataFrame, called_by: str, event: str, planned_run_time_utc: str | None = None) -> pd.DataFrame:

    """
        Modifies the RUN_TYPE file by logging what is run
        Args:
            df_run_type (dataframe) : The dataframe created from the file
            called_by (str): the name of the entry point function which is calling this one, we log it on the run type file
            event (str): the moment of modification of the file (initiate or terminate). 
                    If there is an error during the run, we'll at least have the initiate version logged
            planned_run_time_utc (str containing timestamp): If provided, we log which timestamp of the calendar is planned to be run
        Returns:
            df_run_type modified
        Raises:
            Exits the program if error running the function (using decorator)
    """

    #Get the run time timestamp
    ts_run_time_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") 
    
    #We modify the run_type dataframe
    if event == "initiate":
        row = pd.DataFrame({'RUN_TIME_UTC': [ts_run_time_utc], 
                           'EVENT': [event], 
                           'RUN_TYPE': [called_by], 
                           'RUN_METHOD': [os.getenv("GITHUB_EVENT_NAME")],
                           'OUTPUT_AUTO': [os.getenv("IS_OUTPUT_AUTO")],
                           'PLANNED_RUN_TIME_UTC': [planned_run_time_utc]})
        df_run_type_modified = pd.concat([row,df_run_type], ignore_index=True)
    elif event == "terminate":
        df_run_type_modified = df_run_type
        df_run_type_modified.at[0, 'EVENT'] = event
    if event in ("initiate","terminate"):
        #we (re)create the file after modification
        files_manipulation.create_csv(os.path.join(var.TMPF,"RUN_TYPE.csv"),df_run_type_modified,is_to_encapsulate=var.RUNTYPE_FILE_ENCAPSULATED) 
    
    logging.info("FILE RUN TYPE -> MODIFIED")
    return df_run_type_modified

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('dbt_file_path',)})
def personalize_yml_dbt_file(dbt_file_path: str , sr_snowflake_account: pd.Series):
 
    """
        Personalizes DBT YAML files with snowflake connection attributes   
        Args:
            dbt_file_path (str): Local path of the dbt file
            sr_snowflake_account (series - one row): Contains snowflake connection attributes
        Raises:
            Exits the program if error running the function (using decorator)
    """
    
    text = files_manipulation.read_yml_as_txt(dbt_file_path)

    #We first check if it needs to be personalized
    is_personalized = "#DATABASE#" not in text

    if not(is_personalized): #we personalize it if not

        #We get the environment keys (GitHub secrets)
        SNOWFLAKE_USERNAME = os.getenv('SNOWFLAKE_USERNAME')
        SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD') 
        if os.getenv("IS_TESTRUN") == '0':
            database = sr_snowflake_account['DATABASE_PROD']
        else:
            database = sr_snowflake_account['DATABASE_TEST']

        #we personalize
        text = text.replace("#ACCOUNT#",sr_snowflake_account['ACCOUNT'])
        text = text.replace("#DATABASE#",database)
        text = text.replace("#WAREHOUSE#",sr_snowflake_account['WAREHOUSE'])
        text = text.replace("#USER#",SNOWFLAKE_USERNAME)
        text = text.replace("#PASSWORD#",SNOWFLAKE_PASSWORD)
        files_manipulation.create_yml(dbt_file_path,text)
        logging.info(f"FILE {dbt_file_path} -> PERSONALIZED ")
    else:
        logging.info(f"FILE {dbt_file_path} -> PERSONALIZED - ALREADY DONE")

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def parametrize_yml_dbt_file(dbt_file_path: str):

    """
        Parametrizes DBT YAML files by removing snowflake connection attributes   
        Args:
            dbt_file_path (str): Local path of the dbt file
        Raises:
            Exits the program if error running the function (using decorator)
    """

    text = files_manipulation.read_yml_as_txt(dbt_file_path)

    #we parametrize
    text = re_sub(r'(account:\s*).+', r'\1#ACCOUNT#', text)
    text = re_sub(r'(database:\s*).+', r'\1#DATABASE#', text)
    text = re_sub(r'(password:\s*).+', r'\1#PASSWORD#', text)
    text = re_sub(r'(user:\s*).+', r'\1#USER#', text)

    files_manipulation.create_yml(dbt_file_path,text)
    logging.info(f"FILE {dbt_file_path} -> PARAMETRIZED ")

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def create_json_file_email(**kwargs: str):

    """
        Creates a json file for emailing details if ran by github actions
        Args:
            **kwargs: Variable number of string or None arguments.
        Raises:
            Exits the program if error running the function or not string arg (using decorator)
    """

    for k, v in kwargs.items():
        if not isinstance(v, (str, type(None))):
            raise TypeError(f"Argument '{k}' must be a string, got {type(v).__name__}")

    with open("json_file_email_details.json", "w") as f:
        json.dump(kwargs, f)