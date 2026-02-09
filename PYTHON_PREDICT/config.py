'''
    This module is a utility module for all other modules. It:
    - centralize variables about paths, used along the program
    - define decorators for all program functions
    - define multiprocessing thread to run some function in parallel
'''
import logging
logging.basicConfig(level=logging.INFO)
from time import sleep as time_sleep
from sys import exit as sys_exit
from re import sub as re_sub
import os
from shutil import rmtree as shutil_rmtree
import inspect
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed

# Following is paths parameters
dropbox_folder_root = 'dropbox:prediction_files'
if os.getenv("IS_TESTRUN") == '0':
    dropbox_folder = os.path.join(dropbox_folder_root,"Prod")
    logging.info("Prod run")
else:
    dropbox_folder = os.path.join(dropbox_folder_root,"Test")
    logging.info("Test run")    
paths_file = os.path.join(dropbox_folder_root,'docs/paths.csv')
rclone_config_path = '~/.config/rclone/rclone.conf'
dbt_directory = "../DBT_PREDICT"
dbt_profiles_path = "../DBT_PREDICT/profiles.yml"
dbt_sources_folder = "../DBT_PREDICT/models/sources/"
TMPF = '../TMP_FOLDER'
TMPD = '../TMP_DATABASE'
next_run_time_file_path = os.path.join(dropbox_folder,"current/outputs/python/next_run_time_utc.txt")
trophy_file_path = os.path.join(dropbox_folder_root,'docs/Trophy.JPG')
playoffs_table_code = os.path.join(dropbox_folder_root,'docs/playoffs_table.txt')

# Following is csv file encapsulation parameters
task_done_encapsulated = 0
paths_file_encapsulated = 1
message_encapsulated = 1
runtype_encapsulated = 0
game_encapsulated = 0
need_encapsulated = 0

# Following is time to wait (sec) for external dependencies
dropbox_wait_time = 30
time_message_wait = 90
game_extraction_wait_time = 30
snowflake_login_wait_time = 30

# Following is python maps:
DOWNLOAD_INITIAL_MAP_PER_CALLER = {
    "main": "INITIAL_MAIN",
    "init_snowflake": "INITIAL_SNOWFLAKE",
    "init_compet": "INITIAL_COMPET"
}

DOWNLOAD_ADDITIONAL_MAP = {
    "MESSAGE": "MESSAGE",
    "GAME_RUN": "GAME_RUN"
}

UPLOAD_FOLDER_MAP_PER_CALLER = {
    "main": [TMPF, TMPD],
    "init_compet": [TMPF, TMPD],
    # if called by init_snowflake, we don't need to upload database files, we didn't modify them
    "init_snowflake": [TMPF]
}

DATABASE_RUN_CATEGORY_MAP = {
    "INIT_COMPET": "INIT_COMPET",
    "MESSAGE_CHECK": "MESSAGE_CHECK",
    "MESSAGE_RUN": "MESSAGE_RUN",
    "GAME_RUN": "GAME_RUN",
    "CALCULATION": "CALCULATION"
}

CALLER = {
    "MAIN": "main",
    "SNOWFLAKE": "init_snowflake",
    "COMPET": "init_compet"
}

MESSAGE_ACTION_MAP = {
    "RUN": "RUN",
    "CHECK": "CHECK",
    "AVOID": "AVOID"
}

GAME_ACTION_MAP = {
    "RUN": "RUN",
    "AVOID": "AVOID"
}

TASK_RUN_MAP = {
    "UPDATEGAMES": "UPDATEGAMES",
    "CHECK": "CHECK",
    "CALCULATE": "CALCULATE",
    "INIT": "INIT"
}

DROPBOX_FOLDER_MAP = { 
    "CURRENT" : 'current',
    "-1" :'-1',
    "-2" : '-2',
    "-3" : '-3',
    'global_manual_inputs' : 'global_manual_inputs',
    'local_manual_inputs' : 'local_manual_inputs',
    'manual_current': 'current/inputs/manual'
}

# Following is string parameters used along the program
landing_database_schema = "LANDING"
role_database = "ACCOUNTADMIN"
game_filtering_category = "GAME"
message_filtering_category = "MESSAGE"
message_prefix_program_string = "+++++"
message_prefix_technical_string = "*****"

def onError_final_execute():

    '''
        Executes ultimate code just before exiting the program
        in case of exit_program decorator called at any point in the program
    '''
    
    try:
        
        content = ""
        with open(dbt_profiles_path, 'r', encoding='utf-8') as file:
            content = file.read() 

        #we parametrize
        content = re_sub(r'(account:\s*).+', r'\1#ACCOUNT#', content)
        content = re_sub(r'(database:\s*).+', r'\1#DATABASE#', content)
        content = re_sub(r'(password:\s*).+', r'\1#PASSWORD#', content)
        content = re_sub(r'(user:\s*).+', r'\1#USER#', content)

        with open(dbt_profiles_path, "w", encoding = "utf-8") as file:
            file.write(content)
        logging.info(f"FILE {dbt_profiles_path} -> PARAMETRIZED ")
    except Exception as e:
        logging.error(f"Error: {e}")

    try:
        for folder in [TMPF, TMPD]:
            if os.path.exists(folder):
                shutil_rmtree(folder)
        logging.info(f"FILE -> TMP FOLDER DESTROYED") 
    except Exception as e:
        logging.error(f"Error while destroying local folder: {e}")
    
def exit_program(log_filter=None):
    '''
        Acts as a decorator for most functions if exceptions 
        Exits gracefully the program while logging the error with its context, and executing final function
        Args:
            log_filter (function arguments): this let the decorator log a dict of arguments values which generate the issue.
    '''
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                bound_args = inspect.signature(func).bind(*args, **kwargs)
                bound_args.apply_defaults()
                filtered_args = log_filter(bound_args.arguments) if log_filter else bound_args.arguments

                logging.error(f"Failed for `{func.__name__}` with args: {filtered_args} - Error: {e}")
                onError_final_execute()
                sys_exit(1)
        return wrapper
    return decorator

def retry_function(log_filter=None,max_attempts = 3,delay_secs = 5):

    '''
        Acts as a decorator for external dependencies functions if exceptions (DropBox, SnowFlake,...)
        Retries several times with a delay before giving up
        Args:
            log_filter (function arguments): this let the decorator log a dict of arguments values which generate the issue
        Returns:
            Decorated function with retry mechanism.
    '''
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            bound_args = inspect.signature(func).bind(*args, **kwargs)
            bound_args.apply_defaults()
            filtered_args = log_filter(bound_args.arguments) if log_filter else bound_args.arguments
            
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        logging.error(f"Last attempt failed for `{func.__name__}` with args: {filtered_args}")
                        raise
                    else:
                        logging.error(f"Attempt {attempt}/{max_attempts} failed for `{func.__name__}` with args: {filtered_args}\
                                      Retrying in few seconds.")
                        time_sleep(delay_secs)
                        attempt += 1
        return wrapper
    return decorator

def raise_issue_to_caller(log_filter=None):

    '''
        Acts as a decorator for some functions
        Reraises the issue to the caller with its context, so that it can interact with it with its own decorator
        Args:
            log_filter (function arguments): A function that filters or transforms the argument dictionary
                                      before logging. Defaults to None
        Returns:
            callable: The decorated function
    '''
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            bound_args = inspect.signature(func).bind(*args, **kwargs)
            bound_args.apply_defaults()
            filtered_args = log_filter(bound_args.arguments) if log_filter else bound_args.arguments
            try:
                return func(*args, **kwargs)
            except Exception as e:
                    logging.error(f"Failed for `{func.__name__}` with args: {filtered_args}")
                    raise
        return wrapper
    return decorator 

@exit_program(log_filter=lambda args: {})
def create_local_folder():
    '''
        Creates local folder to download files from DropBox
    '''

    for folder in [TMPF, TMPD]:
        if os.path.exists(folder):
            shutil_rmtree(folder)
        os.makedirs(folder)

    logging.info(f"FILES -> TMP FOLDERS CREATED") 

@exit_program(log_filter=lambda args: {})
def destroy_local_folder():
    '''
        Destroys local folders after files are uploaded on DropBox
    '''

    for folder in [TMPF, TMPD]:
        if os.path.exists(folder):
            shutil_rmtree(folder)

    logging.info(f"FILE -> TMP FOLDER DESTROYED") 

def multithreading_run(fn, fn_args_list,thread_max_workers = 5):

    '''
        Runs function in parallel using several thread
        Args:
            fn: the name of the function we parallelize
            fn_args_list: the name of the function arguments
        Returns:
            The return of the function
    '''
    if len(fn_args_list) == 0:
        return []
    
    results = []
    with ThreadPoolExecutor(thread_max_workers) as executor:
        futures = [executor.submit(fn, *args) for args in fn_args_list]
        for future in as_completed(futures):
            results.append(future.result())
    return results