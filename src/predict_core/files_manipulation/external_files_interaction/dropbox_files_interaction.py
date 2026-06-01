'''
The purpose of this module is to interact with DropBox environment, 
mainly to download files locally for modification then upload them back
'''
import logging
import os
import subprocess
from pathlib import Path
from typing import Literal
import pandas as pd

from ...config import config_decorators
from ...config.config_multithread import multithread_run
from ...config.config_variables import config_global_variables as var
from ..local_files_manipulation import files_manipulation
from ...files_manipulation.external_files_interaction import dropbox_files_interaction as dropbox

logging.basicConfig(level=logging.INFO)

@config_decorators.exit_program(log_filter=lambda args: dict(args))
@config_decorators.retry_function(log_filter=lambda args: dict(args))
def download_file(dropbox_file_path: str, local_folder: str, is_encapsulated: Literal[0, 1] = 0, is_path_abs: Literal[0, 1] = 0) -> dict:

    """
        Technically download files from dropbox to the local environment
         Then returns the python object associated (dataframe or string)
        Args:
            dropbox_file_path (str): The path of the file on DropBox
            local_folder (str): The local folder where to download
            is_encapsulated (0/1): Has the file been encapsulated (with ")? 1= yes, 0=no - default = no
            is_path_abs (0/1): If the dropbox_file_path is already absolute? 1= yes, 0=no - default = no
        Returns:
            a data dictionary containing the python object created (dataframe for csv files or string for txt file)
        Raises:
            Retry 3 times and exits the program if error with rclone (using decorators)
    """

    logging.info(f"DROPBOX {dropbox_file_path} -> DOWNLOADING [START]")
    
    RCLONE_CONFIG_PATH = var.RCLONE_CONFIG_PATH
    #We get the dropbox folder from config if the path is not already absolute
    if is_path_abs == 0:
        DROPBOX_FOLDER = var.DROPBOX_FOLDER.rstrip('/')  
        dropbox_file_path_abs = os.path.join(DROPBOX_FOLDER,dropbox_file_path)
    else:
        dropbox_file_path_abs = dropbox_file_path

    file_name = os.path.basename(dropbox_file_path)
    local_file_path_abs = os.path.join(local_folder, file_name)
    #we download the file only if it is not already downloaded
    if not os.path.exists(local_file_path_abs):

        command = [
            'rclone', 'copy', dropbox_file_path_abs , local_folder,
            '--config', os.path.expanduser(RCLONE_CONFIG_PATH)]

        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=var.DROPBOX_WAIT_TIME)

        #If there is an error downloading file, we raise an error for the retry decorator
        if result.returncode != 0:
            raise ValueError(f"DROPBOX {dropbox_file_path} -> Error downloading file: {result.stderr}")
    else:
        logging.info(f"DROPBOX {dropbox_file_path} -> DOWNLOADING [ALREADY DONE]")
        
    #we get the type of file and return the python object type (df for csv, string for txt or yml file)
    extension = Path(file_name).suffix.lower()
    filename_short = Path(file_name).stem
    files_data_dict = {}
    
    if extension == ".csv":
        files_data_dict['df_'+filename_short.lower()] = files_manipulation.read_and_check_csv(local_file_path_abs,is_encapsulated)        
    elif extension in [".yml", ".yaml"]: #only one level yaml file
        files_data_dict['sr_'+filename_short.lower()] = files_manipulation.read_and_check_yml_as_serie(local_file_path_abs)
    elif extension == ".txt":
        files_data_dict['str_'+filename_short.lower()] = files_manipulation.read_txt(local_file_path_abs)
    
    logging.info(f"DROPBOX {dropbox_file_path} -> DOWNLOADING [DONE]")
    return files_data_dict

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('file_name', 'local_folder')})
def get_locally(file_name: str, local_folder: str, df_paths: pd.DataFrame) -> dict:

    """
        Downloads a file from DropBox, given a file name from the paths file dataframe
        Args:
            file_name (str) : The name of the file (without extension) on the paths file
            local_folder (str): The local folder where to download 
            df_paths (dataframe): The dataframe of the paths file
        Returns:
            data dictionary containing the python object created (dataframe, string)
        Raises:
            Exits the program if error running the function (using decorator)
    """

    #We first get the info related with the file_name
    file_infos = df_paths[df_paths["NAME"] == file_name].iloc[0]
    path = file_infos["PATH"].strip().strip('"')
    is_encapsulated = file_infos["IS_ENCAPSULATED"]

    #We then download it and get the data dictionary returned
    files_data_dict = download_file(dropbox_file_path = path,
                                            local_folder = local_folder,
                                            is_encapsulated = is_encapsulated)
    return files_data_dict

@config_decorators.exit_program(log_filter=lambda args: dict(args))
@config_decorators.retry_function(log_filter=lambda args: dict(args))
def copy_folder(remote_source_folder: str, remote_target_folder: str, sourcepath_from_root: Literal[0, 1] = 0, targetpath_from_root: Literal[0, 1] = 0, sync_folder: Literal[0, 1] = 1):

    """
        Copies all elements from a subfolder on DropBox to another one, via rclone
        Args:
            remote_source_folder (str) : The path of the folder we want to copy from  on Dropbox
            remote_target_folder (str): The path of the folder we want to copy to on DropBox
            sourcepath_from_root (0/1): If 1, we get from root the absolute path of source folder, else no
            targetpath_from_root (0/1): If 1, we get from root the absolute path of target folder, else no
            sync_folder (0/1): If 1 we replace all the folder by the new folder, else we just copy into new folder
        Raises:
            Retries 3 times and exits the program if error with rclone (using decorators)
    """
    
    logging.info(f"DROPBOX {remote_source_folder} -> COPYING FOLDER TO {remote_target_folder} [START]")
    #If path_from_root = 1,We get the dropbox root folder, else the normal dropbox folder
    source_base = var.DROPBOX_FOLDER_ROOT if sourcepath_from_root==1 else var.DROPBOX_FOLDER
    target_base = var.DROPBOX_FOLDER_ROOT if targetpath_from_root==1 else var.DROPBOX_FOLDER

    #we can get absolute path from previous folders
    remote_source_folder_abs = os.path.join(source_base,remote_source_folder)
    remote_target_folder_abs = os.path.join(target_base,remote_target_folder)

    #We list the files from source folder
    command_list = ['rclone', 'lsf', remote_source_folder_abs ]
    result_list = subprocess.run(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=var.DROPBOX_WAIT_TIME)

    #If there is an error listing files, we raise an error for the retry decorator
    if result_list.returncode != 0:
        raise ValueError(f"Error listing files: {result_list.stderr}")

    #Then we copy the files - as there is lots of file we wait longer
    if sync_folder == 1:
        command_copy = ['rclone', 'sync', remote_source_folder_abs, remote_target_folder_abs , '--progress']
    else:
        command_copy = ['rclone', 'copy', remote_source_folder_abs, remote_target_folder_abs , '--progress']
    result_copy = subprocess.run(command_copy, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=2*var.DROPBOX_WAIT_TIME)

    #If there is an error copying files, we raise an error for the retry decorator
    if result_copy.returncode != 0:
        raise ValueError(f"Error copying files: {result_copy.stderr}")
        
    logging.info(f"DROPBOX {remote_source_folder} -> COPYING FOLDER TO {remote_target_folder} [DONE]")

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def initiate_folder():

    """
        Prepares the environnement of files on DropBox:
        - Creating a rotating backup of the current and previous file states:
            Rotation: current -> -1 -> -2 -> -3
        - Copying manual input files into the current input directory
        Raises:
            Exits the program if error running the function (using decorator)
    """
        
    copy_folder(var.DROPBOX_FOLDER_MAP['-2'],var.DROPBOX_FOLDER_MAP['-3'])
    copy_folder(var.DROPBOX_FOLDER_MAP['-1'],var.DROPBOX_FOLDER_MAP['-2'])
    copy_folder(var.DROPBOX_FOLDER_MAP['CURRENT'],var.DROPBOX_FOLDER_MAP['-1'])
    copy_folder(var.DROPBOX_FOLDER_MAP['global_manual_inputs'],var.DROPBOX_FOLDER_MAP['manual_current'],sourcepath_from_root=1,targetpath_from_root=0, sync_folder=0)
    copy_folder(var.DROPBOX_FOLDER_MAP['local_manual_inputs'],var.DROPBOX_FOLDER_MAP['manual_current'], sync_folder=0)

@config_decorators.exit_program(log_filter=lambda args: dict(args))
@config_decorators.retry_function(log_filter=lambda args: dict(args))
def upload_file(local_file_path: str, remote_file_path: str):

    """
        The purpose of this function is to upload a local file to dropbox
        Args:
            local_file_path (str): The path of the file locally
            remote_file_path (str): The path of the file on DropBox
        Raises:
            Retry 3 times and exits the program if error with rclone (using decorators)
    """

    logging.info(f"DROPBOX {local_file_path} -> UPLOADING [START]")
    
    # we extract the remote folder from the remote path - it can already look like a folder ending with /
    if remote_file_path.endswith('/'):
        remote_folder = remote_file_path
    else:
        remote_folder = os.path.dirname(remote_file_path) + '/'

    #we upload
    remote_folder_abs = os.path.join(var.DROPBOX_FOLDER,remote_folder)
    RCLONE_CONFIG_PATH = var.RCLONE_CONFIG_PATH

    command = [
        'rclone', 'copy', local_file_path, remote_folder_abs,
        '--config', os.path.expanduser(RCLONE_CONFIG_PATH)
    ]

    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=var.DROPBOX_WAIT_TIME)

    #If there is an error uploading file, we raise an error for the retry decorator
    if result.returncode != 0:
        raise ValueError(f"DROPBOX {local_file_path} -> Error uploading file: {result.stderr}")

    logging.info(f"DROPBOX {local_file_path} -> UPLOADING [DONE]")

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('folder_name','local_folder')})
@config_decorators.retry_function(log_filter=lambda args: {k: args[k] for k in ('folder_name','local_folder')})
def download_folder(folder_name: str, df_paths: pd.DataFrame, local_folder: str):

    """
        Downloads all files from a folder in DropBox
        Args:
            folder_name (str): The name of the DropBox folder on the paths file
            df_paths (dataframe): The dataframe related to paths file
            local_folder (str): The place where to download files from the dropbox folder locally
        Raises:
            Retry 3 times and exits the program if error with rclone (using retry decorator)
    """

    logging.info(f"DROPBOX: {folder_name} -> DOWNLOADING FOLDER [START]")

    #We get the dropbox folder complete path
    folder_dropbox_path = df_paths[df_paths["NAME"] == folder_name].iloc[0]["PATH"]
    folder_dropbox_path_abs = os.path.join(var.DROPBOX_FOLDER,folder_dropbox_path)

    #We list the files in the dropbox folder
    command_list = ['rclone', 'lsf', folder_dropbox_path_abs ]
    result_list = subprocess.run(command_list, stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE, text=True, timeout=var.DROPBOX_WAIT_TIME)

    #If there is an error listing files, we raise an error for the retry decorator
    if result_list.returncode != 0:
        raise ValueError(f"Error listing files: {result_list.stderr}")

    #Then we download each file using their name
    files = [Path(file).stem for file in result_list.stdout.strip().split("\n") if file.strip()]
    
    # We parallelize the downloading of those files
    file_args = [(file_name, local_folder,df_paths) for file_name in files]
    multithread_run(dropbox.get_locally, file_args)
    
    logging.info(f"DROPBOX: {folder_name} -> DOWNLOADING FOLDER [DONE]")

@config_decorators.exit_program(log_filter=lambda args: {})
def download_needed_files(df_paths: pd.DataFrame, sr_output_need: pd.Series) -> dict :

    """
        Called by entry point main program, it gets the conditional list of files which need to be downloaded
        and downloads them and create objects related (dataframe, string) 
        Args:
            df_paths (dataframe): we extract dropbox paths from the paths file
            sr_output_need (series - one row): related to output_need file
                parameters to know which file to download are here
        Returns:
            data dictionary with all object created
        Raises:
            Exits the program if error running the function (using decorator)
    """
   
    logging.info("FILES -> DOWNLOADING NEEDED FILES [START]")
    
    files_data_dict = {}

    #We get the download category of files we need
    download_category = []
    #if we run/check message we download files with message flagged category
    if sr_output_need['MESSAGE_ACTION'] in (var.MESSAGE_ACTION_MAP['CHECK'],var.MESSAGE_ACTION_MAP['RUN']):
        download_category.extend([var.DOWNLOAD_ADDITIONAL_MAP["MESSAGE"]])
    #if we run game we download files with game flagged category
    if sr_output_need['GAME_ACTION'] in (var.GAME_ACTION_MAP['RUN']):
        download_category.extend([var.DOWNLOAD_ADDITIONAL_MAP["GAME_RUN"]]) 
    #if we want to initiate/ calculate gameday in output, we dowwnload files we need for this
    if sr_output_need.at['TASK_RUN'] in (var.TASK_RUN_MAP['INIT'],var.TASK_RUN_MAP['CALCULATE']):
        download_category.extend([sr_output_need['TASK_RUN']]) 

    #We get list of tables which need to be updated based on possible categories
    files_to_download = df_paths.loc[
    df_paths['DOWNLOAD_CATEGORY'].map(lambda x: any(cat in download_category for cat in x)),'NAME'].tolist()
    
    logging.info("__________________________________________________________________")
    logging.info(f"LIST OF FILES TO BE DOWNLOADED: {files_to_download}")
    logging.info("__________________________________________________________________")

    # We parallelize the downloading of those files
    download_args = [(file_name, var.TMPF, df_paths) for file_name in files_to_download]
    results = multithread_run(get_locally, download_args)
    files_data_dict.update({k: v for r in results for k, v in r.items()})

    logging.info("FILES -> DOWNLOADING NEEDED FILES [END]")
    return files_data_dict