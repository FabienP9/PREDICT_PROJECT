'''
    The purpose of this module is to interact with files by
     - creating and terminating local folders which will store them temporarily
'''
import logging
import os
from shutil import rmtree as shutil_rmtree
from pathlib import Path

from ...config import config_decorators
from ...config.config_multithread import multithread_run
from ...config.config_variables import config_global_variables as var
from . import files_manipulation
from .specific_files_operations import specific_files_operations
from ...files_manipulation.external_files_interaction import dropbox_files_interaction as dropbox

logging.basicConfig(level=logging.INFO)

@config_decorators.exit_program(log_filter=lambda args: {})
def create_local_folder():
    '''
        Creates local folder to store files from DropBox
    '''

    for folder in [var.TMPF, var.TMPD]:
        if os.path.exists(folder):
            shutil_rmtree(folder)
        os.makedirs(folder)

    logging.info("FILES -> TMP FOLDERS CREATED") 

@config_decorators.exit_program(log_filter=lambda args: {})
def destroy_local_folder():
    '''
        Destroys local folders after files are uploaded on DropBox
    '''

    for folder in [var.TMPF, var.TMPD]:
        if os.path.exists(folder):
            shutil_rmtree(folder)

    logging.info("FILE -> TMP FOLDER DESTROYED") 

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def initiate_local_environment(called_by: str) -> dict:

    """
        The purpose of this function is to:
        - create a local folder environment to modify files locally
        - initiate it with "INITIAL" type flagged files according to called_by argument
        - modify local "RUN_TYPE" file and upload it back to dropbox, to log the starting run info
        - personnalize yml dbt files
        Args:
            called_by (str): the name of the function calling this function, will define precisely the flag
        Returns:
            dictionary containing all python objects (dataframe, string) associated with files downloaded from dropbox
        Raises:
            Exits the program if error running the function (using decorator)
    """
    logging.info("FILES -> INITIATE LOCAL ENVIRONMENT [START]")
    context_dict = {}
    
    #We create the local folder to interact with files
    create_local_folder()
    
    #We download the file which contains all file paths on DropBox, to avoid hardcoding every path here
    context_dict.update(specific_files_operations.get_paths_file_details())
    df_paths = context_dict['df_paths']
    
    #We download files we'll need in all cases: flagged "INITIAL_*" on the column "DOWNLOAD_CATEGORY" of paths file
    str_initial_flag = var.DOWNLOAD_INITIAL_MAP_PER_CALLER.get(called_by)
    files_to_download = [
        name for cat_list, name in zip(df_paths['DOWNLOAD_CATEGORY'], df_paths['NAME'])
        if str_initial_flag in cat_list
    ]
    
    logging.info("__________________________________________________________________")
    logging.info(f"LIST OF FILES TO BE DOWNLOADED: {files_to_download}")
    logging.info("__________________________________________________________________")

    # We parallelize the downloading of those files
    download_args = [(file_name, var.TMPF, df_paths) for file_name in files_to_download]
    results = multithread_run(dropbox.get_locally, download_args)
    context_dict.update({k: v for r in results for k, v in r.items()})
    context_dict['sr_snowflake_account_connect'] = context_dict['df_snowflake_account_connect'].iloc[0]
    
    # we copy next_run time to current_run time if called by main 
    # (the only "exe" function using the value):
    # it was the next one of the previous run
    if called_by == var.CALLER["MAIN"]:
        context_dict['str_current_run_time_utc'] = context_dict['str_next_run_time_utc']
    # We then modify and upload back the run type file to log run info on DropBox
        context_dict['df_run_type'] = specific_files_operations.modify_run_type_file(context_dict['df_run_type'],
                                            called_by, 
                                            event = "initiate",
                                            planned_run_time_utc=context_dict['str_current_run_time_utc'])
    else:
        context_dict['df_run_type'] = specific_files_operations.modify_run_type_file(context_dict['df_run_type']
                                                                                    ,called_by, event = "initiate")
    
    remote_path_file_runtype = df_paths[df_paths["NAME"] == "RUN_TYPE"].iloc[0]['PATH']
    dropbox.upload_file(os.path.join(var.TMPF,"RUN_TYPE.csv"),remote_path_file_runtype)
    
    #We personalize profiles.yml file with snowflake connection attributes, for potential dbt run
    specific_files_operations.personalize_yml_dbt_file(var.DBT_PROFILES_PATH,context_dict['sr_snowflake_account_connect'])
    
    #We filter competition file with competition to load
    if called_by == var.CALLER["COMPET"]:
        context_dict['df_competition'] = context_dict['df_competition'][context_dict['df_competition']['IS_TO_LOAD'] == 1]
        files_manipulation.create_csv(os.path.join(var.TMPF,'competition.csv'), context_dict['df_competition'], 0)

    logging.info("FILES -> INITIATE LOCAL ENVIRONMENT [DONE]")
    return context_dict

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def terminate_local_environment(called_by: str, context_dict: dict):

    """
        The purpose of this function is to terminate local folders created for the run:
        - parametrize the dbt files
        - modify local "RUN_TYPE" file and upload it back to dropbox, to log the ending run info
        - upload files from the local environment which need to be uploaded
        - destroy local environment to terminate the program
        Args:
            called_by (str): the name of the function calling this function, 
                will define precisely which folder we check for uploading files in it
            context_dict (data dictionary): all objects used to process
        Raises:
            Exits the program if error running the function (using decorator)
        """
    
    logging.info("FILES -> TERMINATE LOCAL ENVIRONMENT [START]")
    #We parametrize profiles.yml file with snowflake connection attributes, for potential dbt run
    specific_files_operations.parametrize_yml_dbt_file(var.DBT_PROFILES_PATH)
    
    context_dict['df_run_type'] = specific_files_operations.modify_run_type_file(context_dict['df_run_type'],called_by, event = "terminate")

    #we upload files in config folders
    folders = var.UPLOAD_FOLDER_MAP_PER_CALLER.get(called_by)
    df_paths = context_dict['df_paths']
    
    # we get the list of files to upload   
    local_files_to_upload = [] 
    for folder in folders:
        for file in os.scandir(folder):
            if file.is_file():
                local_file_path = file.path
                file_name = Path(file).stem
                extension = Path(file).suffix

                # new jpg files might be created along the program in the run context - we virtually change the file_name to match df_paths' one
                if (extension.lower() == ".jpg"):
                    file_name = '*_jpg'
                # new text file like forumoutput_* might be created along the program in the run context - we virtually change the file_name to match df_paths
                elif (extension.lower() == ".txt") and (file_name.lower().startswith("forumoutput_")):
                    file_name = 'forumoutput_*_txt'
                
                path_details = df_paths[df_paths["NAME"] == file_name].iloc[0]
                is_for_upload = path_details['IS_FOR_UPLOAD']
                remote_file_path = path_details['PATH']

            if is_for_upload:
                local_files_to_upload.extend([(local_file_path,remote_file_path)])
    
    # we parallelize files upload to dropbox
    upload_args = [(local_file_path,remote_file_path) for local_file_path,remote_file_path in local_files_to_upload]
    multithread_run(dropbox.upload_file, upload_args)
    
    #we finally destroy the local environment
    destroy_local_folder()
    logging.info("FILES -> TERMINATE LOCAL ENVIRONMENT [DONE]")