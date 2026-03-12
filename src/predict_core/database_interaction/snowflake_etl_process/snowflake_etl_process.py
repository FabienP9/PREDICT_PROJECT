
'''
The purpose of this module is to interact with Snowflake database:
submitting query either directly from python or using dbt
'''

import logging
import os
import subprocess
from pathlib import Path
from typing import Literal, Tuple
import pandas as pd

from ...config import config_decorators
from ...config.config_multithread import multithread_run
from ...config.config_variables import config_global_variables as var
from . import sql_queries as sql
from ...files_manipulation.local_files_manipulation.files_manipulation import create_csv
from ..snowflake_connection_execution import snowflake_execute

logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
logging.getLogger("sqlglot").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO)

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('called_by','message_action','game_action','calculation_needed')})
def get_list_tables_to_update(called_by: str, df_paths: pd.DataFrame, message_action: str | None = None, game_action: str | None = None, calculation_needed: int | None = None) -> Tuple[list[str], list[str]]:

    """
        Gets the list of tables which need to be updated on Snowflake:
        - landing tables (sources) by python
        - other tables by dbt
        Args:
            called_by (str) : the function which are calling this function - the list depends on it
            df_paths (dataframe): we extract the list from the paths file
            message_action (str) : from output_need file - the list depends on it if provided
            game_action (str): from output_need file - the list depends on it if provided
            calculation_needed (int): from output_need file - the list depends on it if provided
        Returns:
            lst_python_tables: the list of snowflake tables which need to be run by python
            lst_dbt_tables: the list of snowflake tables which need to be run by dbt
        Raises:
            Exits the program if error running the function (using decorator)
    """
   
    logging.info("SNOWFLAKE -> LISTING TABLES TO UPDATE [START]")
    #We get the category of tables we need
    lst_python_category = []
    lst_dbt_category = []

    if called_by == var.CALLER["COMPET"]:
        lst_python_category.extend([var.DATABASE_RUN_CATEGORY_MAP["INIT_COMPET"]])
        lst_dbt_category.extend([var.DATABASE_RUN_CATEGORY_MAP["INIT_COMPET"]])
        
    elif called_by == var.CALLER["MAIN"]:
        if message_action == var.MESSAGE_ACTION_MAP['CHECK']:
            lst_python_category.extend([var.DATABASE_RUN_CATEGORY_MAP["MESSAGE_CHECK"]])
            lst_dbt_category.extend([var.DATABASE_RUN_CATEGORY_MAP["MESSAGE_CHECK"]])

        if message_action == var.MESSAGE_ACTION_MAP["RUN"]:
            lst_python_category.extend([var.DATABASE_RUN_CATEGORY_MAP["MESSAGE_RUN"]])
            lst_dbt_category.extend([var.DATABASE_RUN_CATEGORY_MAP["MESSAGE_CHECK"]])
            lst_dbt_category.extend([var.DATABASE_RUN_CATEGORY_MAP["MESSAGE_RUN"]])

        if game_action == var.GAME_ACTION_MAP["RUN"]:
            lst_python_category.extend([var.DATABASE_RUN_CATEGORY_MAP["GAME_RUN"]])
            lst_dbt_category.extend([var.DATABASE_RUN_CATEGORY_MAP["GAME_RUN"]])
            
        if (message_action==var.MESSAGE_ACTION_MAP["RUN"] and calculation_needed):
            lst_dbt_category.extend([var.DATABASE_RUN_CATEGORY_MAP["CALCULATION"]])
            
    #We get list of tables which need to be updated based on categories
    df_paths_python_exploded = df_paths.explode('PYTHON_CATEGORY')
    lst_python_tables = df_paths_python_exploded[df_paths_python_exploded['PYTHON_CATEGORY'].isin(lst_python_category)]['NAME'].unique().tolist()

    df_paths_dbt_exploded = df_paths.explode('DBT_CATEGORY')
    lst_dbt_tables = df_paths_dbt_exploded[df_paths_dbt_exploded['DBT_CATEGORY'].isin(lst_dbt_category)]['NAME'].unique().tolist()

    if called_by == var.CALLER["MAIN"]:
        #We add the landing table corresponding to output_need file
        lst_python_tables.extend(['landing_output_need'])

    # We remove possibles duplicates
    lst_python_tables = list(set(lst_python_tables))
    lst_dbt_tables = list(set(lst_dbt_tables))

    logging.info("__________________________________________________________________")
    logging.info(f"LIST OF TABLES TO BE UPDATED BY PYTHON: {lst_python_tables}")
    logging.info(f"LIST OF TABLES TO BE UPDATED BY DBT: {lst_dbt_tables}")
    logging.info("__________________________________________________________________")

    logging.info("SNOWFLAKE -> LISTING TABLES TO UPDATE [END]")
    return lst_python_tables,lst_dbt_tables

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('table_metadata',)})
def delete_table_data(sr_snowflake_account: pd.Series, schema: str, table_metadata: list):

    """
        Deletes all data from a snowflake table and its stage
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to run a query
            schema (str): The name of the schema on which we delete
            table_metadata (list): the list returned by snowflake when "showing" the table
        Raises:
            Exits the program if error running the function (using decorator)
    """

    table_name = table_metadata[1]   # Name is in the 2nd column when showing from snowflake
        
    #for each table, we delete data
    q_delete_data = sql.DELETE_DATA_QUERY.replace(sql.SCHEMA,schema).replace(sql.TABLE_NAME,table_name)
    snowflake_execute(sr_snowflake_account,q_delete_data,sql.DATABASE)

    #for each table stage @%, we delete files
    q_remove_from_stage = sql.REMOVE_FROM_STAGE_QUERY.replace(sql.SCHEMA,schema).replace(sql.TABLE_NAME,table_name)
    snowflake_execute(sr_snowflake_account,q_remove_from_stage,sql.DATABASE)
    logging.info(f"SNOWFLAKE {table_name.upper()} -> DATA DELETED")

@config_decorators.exit_program(log_filter=lambda args: {})
def delete_tables_data_from_python(sr_snowflake_account: pd.Series, schema: str):

    """
        Deletes all data from a snowflake schema objects (tables and stages)
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to run a query
            schema (str): The name of the schema on which we delete
        Raises:
            Exits the program if error running the function (using decorator)
    """

    logging.info(f"SNOWFLAKE {schema} -> DELETING DATA [START]")
    
    #we list all tables in the schema
    q_list_tables = sql.LIST_TABLES_QUERY.replace(sql.SCHEMA,schema)
    lst_tables = snowflake_execute(sr_snowflake_account,q_list_tables,sql.DATABASE)

    # We parallelize the data deletion of those tables and their stages
    table_args = [(sr_snowflake_account,schema,table_metadata) for table_metadata in lst_tables]
    multithread_run(delete_table_data, table_args)

    logging.info(f"SNOWFLAKE {schema} -> DELETING DATA [DONE]")

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('table_name','is_encapsulated')})
def create_table_file(sr_snowflake_account: pd.Series, table_name: str, is_encapsulated: Literal[0, 1]):

    """
        Select data from a snowflake table and create the csv file related
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to run a query
            table_name (str): The name of the table for which we create the file
            is_encapsulated (0/1): To know if the file needs to be encapsulated (") while creating it - 0=no, 1=yes
        Raises:
            Exits the program if error running the function (using decorator)

    """
    #We get the schema at the beginning of the table_name
    schema = table_name.split('_')[0]

    q_select_data = sql.SELECT_TABLE_QUERY.replace(sql.SCHEMA,schema).replace(sql.TABLE_NAME,table_name)
    df = snowflake_execute(sr_snowflake_account,q_select_data,sql.DATABASE)

    #we create the csv file
    create_csv(os.path.join(var.TMPD,table_name)+'.csv',df,is_encapsulated)    

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('called_by','local_folder')})
def update_snowflake_from_python(called_by: str, sr_snowflake_account: pd.Series, table_name: str, df_paths: pd.DataFrame, local_folder: str):

    """
        The purpose of this function is to:
        -  update a snowflake table and its stage from a python script using an input file
            * when called by main or init_compet, the input file created by python have the name of the table, minus "landing_"
            * when called by init_snowflake, the input file has the same name, as we downloaded the table file directly from dropbox
        -  create a csv file of the updated table (only when called by main or init_compet, we already have it when called by init_snowflake)
        Args:
            called_by (str): The entry point function calling this function
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to run a query
            table_name (str): The name of the table we update
            df_paths (dataframe): the paths of files, to know if files are encapsulated
            local_folder (str): The local folder containing the file used to fill the table
        Raises:
            Exits the program if error running the function (using decorator)
    """
    logging.info(f"SNOWFLAKE {table_name} -> UPDATING FROM PYTHON [START]")
    #we get schema and file related info
    schema = table_name.split('_')[0]

    #if called by main or init_compet, the input file have the same name than the table minus the first part "landing_"
    if called_by in (var.CALLER["MAIN"],var.CALLER["COMPET"]):
        file_name = '_'.join(table_name.split('_')[1:])
    #if not, the input file is the same name
    elif called_by == var.CALLER["SNOWFLAKE"]:
        file_name = table_name

    #we get info about the file
    file_path = os.path.join(local_folder,file_name+'.csv')
    file_path_abs = Path(file_path).resolve()
    is_encapsulated = df_paths.loc[df_paths['NAME'] == table_name, 'IS_ENCAPSULATED'].iloc[0]

    #we update stage and table
    q_insert_data = sql.INSERT_DATA_QUERY.replace(sql.SCHEMA,schema).replace(sql.TABLE_NAME,table_name)
    if (is_encapsulated == 1):
         q_insert_data = q_insert_data.replace("#ISENCLOSED#", 
                                           "FIELD_OPTIONALLY_ENCLOSED_BY=\'\"\' NULL_IF = (\'\', \'NULL\')")
    else:
         q_insert_data = q_insert_data.replace("#ISENCLOSED#", "")

    q_put_to_stage = sql.PUT_TO_STAGE_QUERY.replace("#FILE_PATH_ABS#",str(file_path_abs)).replace(sql.SCHEMA,schema).replace(sql.TABLE_NAME,table_name)
    snowflake_execute(sr_snowflake_account,q_put_to_stage,sql.DATABASE)
    snowflake_execute(sr_snowflake_account,q_insert_data,sql.DATABASE)
    #if called by main or init_compet, we need to create the file from the table
    if called_by in [var.CALLER["MAIN"],var.CALLER["COMPET"]]:
        create_table_file(sr_snowflake_account, table_name, is_encapsulated)
    logging.info(f"SNOWFLAKE {table_name} -> UPDATING FROM PYTHON [DONE]")

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('called_by','lst_dbt_tables')})
def update_snowflake_from_dbt(called_by: str, sr_snowflake_account: pd.Series, df_paths: pd.DataFrame,  lst_dbt_tables: list[str] | None = None):
    
    '''
        The purpose of this function is to:
         - call dbt and run a command to update Snowflake tables, according to the entry point function calling it
         - download results of tables updated into csv, according to the entry point function calling it
        Inputs:
            called_by (str): The entry point function calling this one
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to download tables into csv
            df_paths (dataframe): containing details about tables to download them
        Raises:
            If there is an error running dbt we directly exit the program with decorator
    '''
    
    logging.info("SNOWFLAKE -> UPDATING TABLES FROM DBT [START]")

    if called_by == var.CALLER["SNOWFLAKE"]:
        # we just run seeds and views
        dbt_command = "dbt build --select config.materialized:seed + config.materialized:view --exclude test_type:unit --fail-fast"
        bl_select_table = False
    else:
        # we run all tables from the list
        dbt_command = "dbt build --select source:* +"  + " ".join(lst_dbt_tables) + " --exclude test_type:unit --fail-fast"
        bl_select_table = True
    
    base_path = os.path.abspath(var.DBT_DIRECTORY) 
    os.environ["DBT_PROFILES_DIR"] = os.path.join(os.path.dirname(base_path), var.DBT_DIRECTORY)

    # Common parameters for subprocess.run
    run_params = {
        "cwd": var.DBT_DIRECTORY,
        "shell": True,
        "text": True,
        "capture_output": True,
        "env": os.environ.copy()
    }
    logging.info(f"SNOWFLAKE -> RUNNING: {dbt_command}")

    # we run dbt command
    result = subprocess.run(dbt_command, **run_params)
    if result.returncode != 0:
        raise RuntimeError(f"DBT command failed:\n{result.stdout.strip()}\n{result.stderr.strip()}")
    logging.info(f"DBT command passed:\n{result.stdout.strip()}")

    if bl_select_table:
        # except for call_by init_snowflake, we create csv files locally related to table
        # for init_snowlake we already have the files as we inserted data from them
        for table_name in lst_dbt_tables:
            is_encapsulated = df_paths.loc[df_paths['NAME'] == table_name, 'IS_ENCAPSULATED'].iloc[0]
            create_table_file(sr_snowflake_account, table_name, is_encapsulated)

    logging.info("SNOWFLAKE -> UPDATING TABLES FROM DBT [DONE]")

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('called_by','local_folder')})
def update_snowflake(called_by: str, context_dict: dict, local_folder: str):

    """
        The purpose of this function is to:
        -  get the list of snowflake objects that needs to be run, according to the entry point function calling it
        -  run them either from python or dbt
        Args:
            called_by (str) : The entry point function calling this one
            context_dict (data dictionary): The data dictionary containing main run objects:
                some of them are used to get the list of snowflake objects to update
            local_folder (str): The local folder where we download the files related to updated tables
        Raises:
            Exits the program if error running the function (using decorator)
    """

    df_paths = context_dict['df_paths']
    sr_snowflake_account = context_dict['sr_snowflake_account_connect']
    
    #we get the list of table we should update - except if called_by init_snowflake: we don't need it
    if called_by == var.CALLER["MAIN"]:
        message_action = context_dict['sr_output_need']['MESSAGE_ACTION']
        game_action = context_dict['sr_output_need']['GAME_ACTION']
        calculation_needed = (context_dict['sr_output_need']['IS_TO_CALCULATE'] + context_dict['sr_output_need']['IS_TO_DELETE'] + context_dict['sr_output_need']['IS_TO_RECALCULATE'] > 0)
        lst_python_tables,lst_dbt_tables = get_list_tables_to_update(called_by,df_paths,message_action,game_action,calculation_needed)
    elif called_by == var.CALLER["COMPET"]:
        lst_python_tables,lst_dbt_tables = get_list_tables_to_update(called_by,df_paths)
    
    # we update tables from python from lst_python_tables 
    # and dbt with lst_dbt_tables - except for init_snowflake
    if called_by in (var.CALLER["MAIN"],var.CALLER["COMPET"]):
        
        if len(lst_python_tables) != 0:
            
            table_args = [(called_by, 
                        sr_snowflake_account,
                        table_name,
                        df_paths,
                        local_folder) for table_name in lst_python_tables]
            multithread_run(update_snowflake_from_python, table_args)
        
        if len(lst_dbt_tables) != 0:
            update_snowflake_from_dbt(called_by, sr_snowflake_account, df_paths, lst_dbt_tables) 
        
    # in this case we do it from python with the list of downloaded table files
    elif called_by == var.CALLER["SNOWFLAKE"]:
        file_args = [(called_by, 
                sr_snowflake_account,
                Path(file).stem,
                df_paths,
                local_folder) for file in os.listdir(local_folder)]
        multithread_run(update_snowflake_from_python, file_args)

        # no direct tables to update from dbt as we just copied all data from the files into the related tables
        # we call dbt to create seeds and views
        update_snowflake_from_dbt(called_by,sr_snowflake_account,df_paths)