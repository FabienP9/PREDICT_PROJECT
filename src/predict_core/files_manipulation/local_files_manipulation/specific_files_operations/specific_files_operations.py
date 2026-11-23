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

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def create_json_file_email():

    """
        Creates a json file for emailing details if ran by github actions
        Args:
            **kwargs: Variable number of string or None arguments.
        Raises:
            Exits the program if error running the function or not string arg (using decorator)
    """

    with open("json_file_email_details.json", "w") as f:
        json.dump(var.JSON_EMAIL_FILE_DICT, f, default=str)