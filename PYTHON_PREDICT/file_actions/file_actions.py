'''
    The purpose of this module is to interact with files by
     - creating and terminating local environment
     - creating files and reading files coming from DropBox
     - possibly personalizing them with contextual variables
     - modifying and filtering them
'''
import logging
logging.basicConfig(level=logging.INFO)
import pandas as pd
import csv
import os
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent
import ast
import networkx as nx
from re import sub as re_sub
import json
from datetime import datetime, timezone
from typing import Literal
from matplotlib.figure import Figure

import dropbox_actions as dropboxA
import config

@config.exit_program(log_filter=lambda args: dict(args))
def read_json(local_file_path: str) -> list :

    """
        Reads the list from a json file and returns the content
        Args:
            local_file_path (str): Local path to the json file
        Returns:
            The content of the json file (list)
        Raises:
            Exits the program if error running the function (using decorator)
    """

    with open(local_file_path, 'r', encoding='utf-8') as file:
        lst = json.load(file)
    return lst

@config.exit_program(log_filter=lambda args: dict(args))
def read_and_check_csv(local_file_path: str, is_encapsulated: Literal[0, 1] = 0) -> pd.DataFrame:
    """
        Calls the read_csv function from pandas and return the dataframe
        if all expected columns are there
        Args:
            local_file_path (str): Local path to the csv file
            is_encapsulated (0/1): Has the file been encapsulated (with ")? 1= yes, 0=no - default = no
        Returns:
            The dataframe of the csv
        Raises:
            Exits the program if error running the function or if columns not found (using decorator)
    """
    if is_encapsulated==1:
        df = pd.read_csv(local_file_path,header=0,quotechar='"')
    else:
        df = pd.read_csv(local_file_path,header=0)

    filename = Path(local_file_path).name
    expected_columns = read_json(BASE_DIR / "file_check.json")["schemas"].get(filename, {}).get("columns", {})
    actual_columns = df.columns.tolist()
    missing = [col for col in expected_columns if col not in actual_columns]
    if missing:
        raise ValueError(f"Columns missing in {filename}: {missing}")

    type_mismatches = []
    for col, expected_type in expected_columns.items():
        actual_type = str(df[col].dtype)

        if df[col].isna().all() and expected_type == "object":
            continue  

        if actual_type != expected_type:
            type_mismatches.append((col, expected_type, actual_type))

    if type_mismatches:
        mismatch_msgs = [f"{col}: expected {exp}, got {act}" for col, exp, act in type_mismatches]
        raise ValueError(f"Type mismatches in {filename}: {mismatch_msgs}")
    
    return df
    
@config.exit_program(log_filter=lambda args: dict(args))
def read_yml(local_file_path: str) -> str:
    """
        Reads the text from a yml file and returns the content in str format
        Args:
            local_file_path (str): Local path to the YML file
        Returns:
            The content of the yaml file (str)
        Raises:
            Exits the program if error running the function (using decorator)
    """
    content = ""
    with open(local_file_path, 'r', encoding='utf-8') as file:
        content = file.read() 
    return content

@config.exit_program(log_filter=lambda args: dict(args))
def read_txt(local_file_path: str) -> str:
    """
        Reads the text from a txt file and returns the content
        Args:
            local_file_path (str): Local path to the txt file
        Returns:
            The content of the txt file (str)
        Raises:
            Exits the program if error running the function (using decorator)
    """
    content = ""
    with open(local_file_path, 'r', encoding='utf-8') as file:
        content = file.read() 
    return content

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('local_file_path', 'is_to_encapsulate') })
def create_csv(local_file_path: str ,df: pd.DataFrame, is_to_encapsulate: Literal[0, 1] = 0):

    """
        The purpose of this function is to create a csv file using the pandas to_csv method    
        Args:
            local_file_path (str): Path where the CSV file will be saved
            df (dataframe): DataFrame to write to CSV
            is_to_encapsulate (0/1): If 1, encapsulate fields with "". Default is 0 (no encapsulation)
        Raises:
            Exits the program if error running the function (using decorator)
    """
    if is_to_encapsulate == 1:
        df.to_csv(local_file_path , index=False, quotechar='"', quoting=csv.QUOTE_ALL, encoding='utf-8', header=True)
    else:
        df.to_csv(local_file_path, index=False, encoding='utf-8',header=True)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('local_file_path',) })
def create_yml(local_file_path: str, text: str):

    """
        Creates a YAML file from a string (already yml-formatted)
        Args:
            local_file_path (str) : The local path of the file
            text (str) : The string to write
        Raises:
            Exits the program if error running the function (using decorator)
    """
    with open(local_file_path, "w", encoding = "utf-8") as file:
        file.write(text)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('local_file_path',) })
def create_txt(local_file_path: str, text: str):
    """
        Creates a text file from a string
        Args:
            local_file_path (str) : The local path of the file
            text (str) : The string to write
        Raises:
            Exits the program if error running the function (using decorator)
    """
    with open(local_file_path, "w", encoding="utf-8") as file:
        file.write(text)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('local_file_path',) })
def create_jpg(local_file_path: str, fig: Figure):

    """
        Creates a jpg file from a figure
        Args:
            local_file_path (str) : The local path of the file
            fig (matplotlib figure) : The figure to write
        Raises:
            Exits the program if error running the function (using decorator)
    """
    fig.tight_layout()
    fig.savefig(local_file_path, facecolor=fig.get_facecolor(), format='jpg', dpi=150, bbox_inches='tight')

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('dbt_file_path',)})
def personalize_yml_dbt_file(dbt_file_path: str , sr_snowflake_account: pd.Series):
 
    """
        Personalizes DBT YAML files with snowflake connection attributes   
        Args:
            dbt_file_path (str): Local path of the dbt file
            sr_snowflake_account (series - one row): Contains snowflake connection attributes
        Raises:
            Exits the program if error running the function (using decorator)
    """
    
    text = read_yml(dbt_file_path)

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
        create_yml(dbt_file_path,text)
        logging.info(f"FILE {dbt_file_path} -> PERSONALIZED ")
    else:
        logging.info(f"FILE {dbt_file_path} -> PERSONALIZED - ALREADY DONE")

@config.exit_program(log_filter=lambda args: dict(args))
def parametrize_yml_dbt_file(dbt_file_path: str):

    """
        Parametrizes DBT YAML files by removing snowflake connection attributes   
        Args:
            dbt_file_path (str): Local path of the dbt file
        Raises:
            Exits the program if error running the function (using decorator)
    """

    text = read_yml(dbt_file_path)

    #we parametrize
    text = re_sub(r'(account:\s*).+', r'\1#ACCOUNT#', text)
    text = re_sub(r'(database:\s*).+', r'\1#DATABASE#', text)
    text = re_sub(r'(password:\s*).+', r'\1#PASSWORD#', text)
    text = re_sub(r'(user:\s*).+', r'\1#USER#', text)

    create_yml(dbt_file_path,text)
    logging.info(f"FILE {dbt_file_path} -> PARAMETRIZED ")

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('filtering_category',) })
def filter_data(files_data_dict: dict, df_paths: pd.DataFrame, filtering_category: str) -> dict:

    '''
    The purpose of this function is:
    - identify which dataframes need filtering
    - apply filtering rules based on another (already filtered if needed) dataframe.
    - (re)create the csv file related
    Args:
        files_data_dict : The list of objects which might be filtered
        df_paths (dataframe) : The paths dataframe - to know the filtering rules 
        filtering_category (str) : the category of files on df_paths which will be filtered
    Returns:
        a data dictionary with all objects filtered
    Raises:
        Exits the program if error running the function (using decorator)
    '''

    logging.info(f"FILES CATEGORY {filtering_category} -> FILTERING DATA [START]")

    #we get all files which can be filtered
    df_files_filter = df_paths[df_paths['FILTERING_CATEGORY'] == filtering_category].reset_index(drop=True)
    
    #some files reduce their scope using other files already scope reduced
    #we sort df_files_filter such as the one which use another file to be filtered (column FILTERING_FILE) is always sorted later
    def sort_dependency_relationships(df_files_filter):
        # Build a mapping from file names to their row indices
        files_to_index = {val: idx for idx, val in df_files_filter['NAME'].items()}

        # Create a directed graph
        G = nx.DiGraph()

        # Add all indices as nodes
        G.add_nodes_from(df_files_filter.index)

        # Add edges from the row where file name == FILTERING_FILE to the row where FILTERING_FILE == file_name
        for idx, b_val in df_files_filter['FILTERING_FILE'].items():
            if b_val in files_to_index:
                G.add_edge(files_to_index[b_val], idx)

        # Topological sort
        sorted_indices = list(nx.topological_sort(G))

        # Reorder the DataFrame
        df_filtered_sorted = df_files_filter.loc[sorted_indices].reset_index(drop=True)
        return df_filtered_sorted

    df_files_filter = sort_dependency_relationships(df_files_filter)
    
    for _, row in df_files_filter.iterrows():
        df_key = 'df_'+row['NAME']
        #for each existing file having a filtering rule
        if df_key in files_data_dict and len(row['FILTERING_COLUMN']) > 0 and row['FILTERING_FILE'] != "":
            df_to_filter = files_data_dict[df_key]
            df_filtering = files_data_dict['df_'+row['FILTERING_FILE']][row['FILTERING_COLUMN']].drop_duplicates()
            #we merge with the filtering dataframe
            files_data_dict[df_key] = df_to_filter.merge(df_filtering, 
                                        on= row['FILTERING_COLUMN'], 
                                        how='inner')

            #then we (re)create the csv corresponding to the filtered dataframe
            local_file_path = os.path.join(config.TMPF,row['NAME']+'.csv')
            create_csv(local_file_path,files_data_dict[df_key],row['IS_FOR_UPLOAD'])
            logging.info(f"FILE {row['NAME']} -> FILTERED")
            
    logging.info(f"FILES CATEGORY {filtering_category} -> FILTERING DATA [DONE]")
    return files_data_dict

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('file_name', 'local_folder')})
def get_locally_from_dropbox(file_name: str, local_folder: str, df_paths: pd.DataFrame) -> dict:

    """
        Downloads a file from DropBox, given a file name
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
    files_data_dict = dropboxA.download_file(dropbox_file_path = path,
                                            local_folder = local_folder,
                                            is_encapsulated = is_encapsulated)
    return files_data_dict

@config.exit_program(log_filter=lambda args: dict(args))
def modify_run_file(df_RUN_TYPE: pd.DataFrame, called_by: str, event: str, planned_run_time_utc: str | None = None) -> pd.DataFrame:

    """
        Modifies the RUN_TYPE file by logging what is run
        Args:
            df_RUN_TYPE (dataframe) : The dataframe created from the file
            called_by (str): the name of the exe function which is calling this one, we log it on the file
            event (str): the moment of modification of the file (initiate or terminate). 
                    If there is an error during the run, we'll at least have the initiate version logged
            planned_run_time_utc (str containing timestamp): If provided, we log which timestamp of the calendar is planned to be run
        Returns:
            df_RUN_TYPE modified
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
        df_RUN_TYPE_MODIFIED = pd.concat([row,df_RUN_TYPE], ignore_index=True)
    elif event == "terminate":
        df_RUN_TYPE_MODIFIED = df_RUN_TYPE
        df_RUN_TYPE_MODIFIED.at[0, 'EVENT'] = event
    if event in ("initiate","terminate"):
        #we (re)create the file after modification
        create_csv(os.path.join(config.TMPF,"RUN_TYPE.csv"),df_RUN_TYPE_MODIFIED,is_to_encapsulate=config.runtype_encapsulated) 
    
    logging.info(f"FILE RUN TYPE -> MODIFIED")
    return df_RUN_TYPE_MODIFIED

@config.exit_program(log_filter=lambda args: dict(args))
def download_paths_file() -> dict:

    """
        Downloads the file of paths from DropBox to avoid hardcoding all paths here and create the dataframe related
        Returns:
            data dictionary containing the dataframe created - for later use
        Raises:
            Exits the program if error running the function (using decorator)
    """
    
    #We download the file which contains all file paths on DropBox, to avoid hardcoding every path here
    files_data_dict = dropboxA.download_file(dropbox_file_path = config.paths_file,
                                            local_folder = config.TMPF,
                                            is_encapsulated=config.paths_file_encapsulated,
                                            is_path_abs=1)
    
    #We change the type of some of its columns to a list
    columns_to_convert = ["FILTERING_COLUMN", "DOWNLOAD_CATEGORY", "PYTHON_CATEGORY", "DBT_CATEGORY"]
    for col in columns_to_convert:
        files_data_dict['df_paths'][col] = files_data_dict['df_paths'][col].map(
        lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else x
    )

    return files_data_dict

@config.exit_program(log_filter=lambda args: dict(args))
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
    logging.info(f"FILES -> INITIATE LOCAL ENVIRONMENT [START]")
    context_dict = {}
    
    #We create the local folder to interact with files
    config.create_local_folder()
    
    #We download the file which contains all file paths on DropBox, to avoid hardcoding every path here
    context_dict.update(download_paths_file())
    df_paths = context_dict['df_paths']
    
    #We download files we'll need in all cases: flagged "INITIAL_*" on the column "DOWNLOAD_CATEGORY" of paths file
    str_initial_flag = config.DOWNLOAD_INITIAL_MAP_PER_CALLER.get(called_by)
    files_to_download = [
        name for cat_list, name in zip(df_paths['DOWNLOAD_CATEGORY'], df_paths['NAME'])
        if str_initial_flag in cat_list
    ]
    
    logging.info("__________________________________________________________________")
    logging.info(f"LIST OF FILES TO BE DOWNLOADED: {files_to_download}")
    logging.info("__________________________________________________________________")

    # We parallelize the downloading of those files
    download_args = [(file_name, config.TMPF, df_paths) for file_name in files_to_download]
    results = config.multithreading_run(get_locally_from_dropbox, download_args)
    context_dict.update({k: v for r in results for k, v in r.items()})
    context_dict['sr_snowflake_account_connect'] = context_dict['df_snowflake_account_connect'].iloc[0]
    
    # we copy next_run time to current_run time if called by main 
    # (the only "exe" function using the value):
    # it was the next one of the previous run
    if called_by == config.CALLER["MAIN"]:
        context_dict['str_current_run_time_utc'] = context_dict['str_next_run_time_utc']
    # We then modify and upload back the run type file to log run info on DropBox
        context_dict['df_RUN_TYPE'] = modify_run_file(context_dict['df_RUN_TYPE'],
                                            called_by, 
                                            event = "initiate",
                                            planned_run_time_utc=context_dict['str_current_run_time_utc'])
    else:
        context_dict['df_RUN_TYPE'] = modify_run_file(context_dict['df_RUN_TYPE'],called_by, event = "initiate")
    
    remote_path_file_runtype = df_paths[df_paths["NAME"] == "RUN_TYPE"].iloc[0]['PATH']
    dropboxA.upload_file(os.path.join(config.TMPF,"RUN_TYPE.csv"),remote_path_file_runtype)
    
    #We personalize profiles.yml file with snowflake connection attributes, for potential dbt run
    personalize_yml_dbt_file(config.dbt_profiles_path,context_dict['sr_snowflake_account_connect'])
    
    #We filter competition file with competition to load
    if called_by == config.CALLER["COMPET"]:
        context_dict['df_competition'] = context_dict['df_competition'][context_dict['df_competition']['IS_TO_LOAD'] == 1]
        create_csv(os.path.join(config.TMPF,'competition.csv'), context_dict['df_competition'], 0)

    logging.info(f"FILES -> INITIATE LOCAL ENVIRONMENT [DONE]")
    return context_dict

@config.exit_program(log_filter=lambda args: dict(args))
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
    
    logging.info(f"FILES -> TERMINATE LOCAL ENVIRONMENT [START]")
    #We parametrize profiles.yml file with snowflake connection attributes, for potential dbt run
    parametrize_yml_dbt_file(config.dbt_profiles_path)
    
    context_dict['df_RUN_TYPE'] = modify_run_file(context_dict['df_RUN_TYPE'],called_by, event = "terminate")

    #we upload files in config folders
    folders = config.UPLOAD_FOLDER_MAP_PER_CALLER.get(called_by)
    df_paths = context_dict['df_paths']
    
    # we get the list of files to upload   
    local_files_to_upload = [] 
    for folder in folders:
        for file in os.scandir(folder):
            local_file_path = os.path.join(folder,file)
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
    config.multithreading_run(dropboxA.upload_file, upload_args)
    
    #we finally destroy the local environment
    config.destroy_local_folder()
    logging.info(f"FILES -> TERMINATE LOCAL ENVIRONMENT [DONE]")

@config.exit_program(log_filter=lambda args: {})
def download_needed_files(df_paths: pd.DataFrame, sr_output_need: pd.Series) -> dict :

    """
        Called by "exe" main program, it gets the conditional list of files which need to be downloaded
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
   
    logging.info(f"FILES -> DOWNLOADING NEEDED FILES [START]")
    
    files_data_dict = {}

    #We get the download category of files we need
    download_category = []
    #if we run/check message we download files with message flagged category
    if sr_output_need['MESSAGE_ACTION'] in (config.MESSAGE_ACTION_MAP['CHECK'],config.MESSAGE_ACTION_MAP['RUN']):
        download_category.extend([config.DOWNLOAD_ADDITIONAL_MAP["MESSAGE"]])
    #if we run game we download files with game flagged category
    if sr_output_need['GAME_ACTION'] in (config.GAME_ACTION_MAP['RUN']):
        download_category.extend([config.DOWNLOAD_ADDITIONAL_MAP["GAME_RUN"]]) 
    #if we want to initiate/ calculate gameday in output, we dowwnload files we need for this
    if sr_output_need.at['TASK_RUN'] in (config.TASK_RUN_MAP['INIT'],config.TASK_RUN_MAP['CALCULATE']):
        download_category.extend([sr_output_need['TASK_RUN']]) 

    #We get list of tables which need to be updated based on possible categories
    files_to_download = df_paths.loc[
    df_paths['DOWNLOAD_CATEGORY'].map(lambda x: any(cat in download_category for cat in x)),'NAME'].tolist()
    
    logging.info("__________________________________________________________________")
    logging.info(f"LIST OF FILES TO BE DOWNLOADED: {files_to_download}")
    logging.info("__________________________________________________________________")

    # We parallelize the downloading of those files
    download_args = [(file_name, config.TMPF, df_paths) for file_name in files_to_download]
    results = config.multithreading_run(get_locally_from_dropbox, download_args)
    files_data_dict.update({k: v for r in results for k, v in r.items()})

    logging.info(f"FILES -> DOWNLOADING NEEDED FILES [END]")
    return files_data_dict