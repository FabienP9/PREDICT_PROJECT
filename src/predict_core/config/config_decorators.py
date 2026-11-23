'''
    This module is a utility module for all other modules. 
    It defines decorators for all program functions to manage errors and retries
'''
import functools
import inspect
import logging
import os
from re import sub as re_sub
from shutil import rmtree as shutil_rmtree
from typing import Literal
from sys import exit as sys_exit
from time import sleep as time_sleep
import json

from .config_variables import config_global_variables as var

logging.basicConfig(level=logging.INFO)

def execute_finally_on_error():

    '''
        Executes ultimate code just before exiting the program
        in case of exit_program decorator called at any point in the program
    '''
    
    try:
        
        content = ""
        with open(var.DBT_PROFILES_PATH, 'r', encoding='utf-8') as file:
            content = file.read() 

        #we parametrize
        content = re_sub(r'(account:\s*).+', r'\1#ACCOUNT#', content)
        content = re_sub(r'(database:\s*).+', r'\1#DATABASE#', content)
        content = re_sub(r'(password:\s*).+', r'\1#PASSWORD#', content)
        content = re_sub(r'(user:\s*).+', r'\1#USER#', content)

        with open(var.DBT_PROFILES_PATH, "w", encoding = "utf-8") as file:
            file.write(content)
        logging.info(f"FILE {var.DBT_PROFILES_PATH} -> PARAMETRIZED ")

    except Exception as e:
        logging.exception(f"Error - execute finally on error - while parametrizing dbt profile: {e}")

    try:
        with open("json_file_email_details.json", "w") as f:
            json.dump(var.JSON_EMAIL_FILE_DICT, f, default=str)
    except Exception as e:
        logging.exception(f"Error - execute finally on error - while creating email file: {e}") 

    try:
        for folder in [var.TMPF, var.TMPD]:
            if os.path.exists(folder):
                shutil_rmtree(folder)
        logging.info("FILE -> TMP FOLDER DESTROYED") 
    except Exception as e:
        logging.exception(f"Error - execute finally on error - while destroying local temporary folder: {e}")
    
def exit_program(log_filter=None, execute_final_function: Literal[0, 1] = 1, is_soft_error: Literal[0, 1] = 0):
    '''
        Acts as a decorator for most functions if exceptions 
        Exits gracefully the program while logging the error with its context, and executing final function
        Args:
            log_filter (function arguments): this let the decorator log a dict of arguments values which generate the issue
            execute_final_function (0/1): If 1 (defaullt), will call the function execute_finally_on_error
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

                if is_soft_error == 1 or var.SOFT_ERROR_TAG in str(e):
                    error_str = f"Needs retrying because of error with `{func.__name__}` with args: {filtered_args} - Error: {e}"
                    logging.warning(error_str)
                    var.JSON_EMAIL_FILE_DICT['Status of run'] = 'To retry'
                    exit_type = 0 # acts as a sucess
                else:
                    error_str = f"Failed for `{func.__name__}` with args: {filtered_args} - Error: {e}"
                    logging.exception(error_str)
                    var.JSON_EMAIL_FILE_DICT['Status of run'] = 'Failed'
                    exit_type = 1 # acts as a failure
                
                var.JSON_EMAIL_FILE_DICT['Error message'] = error_str
                if execute_final_function == 1:
                    execute_finally_on_error()
                sys_exit(exit_type)

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
                except Exception:
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
            except Exception:
                    logging.error(f"Failed for `{func.__name__}` with args: {filtered_args}")
                    raise
        return wrapper
    return decorator