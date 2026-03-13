
'''
The purpose of this module is to interact with Snowflake database:
submitting query either directly from python or using dbt
'''

import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
import os
import subprocess
from pathlib import Path
import pandas as pd
from typing import Sequence, Mapping, Any, Tuple, Literal
import sqlglot
from sqlglot import exp
logging.getLogger("sqlglot").setLevel(logging.ERROR)

import config
from file_actions.file_actions import create_csv
import sql_queries as sqlQ

# Global variable to store the Snowflake connection
current_snowflake_connection = None

@config.exit_program(log_filter=lambda args: {})
@config.retry_function(log_filter=lambda args: {})
def snowflake_connect(sr_snowflake_account: pd.Series) -> SnowflakeConnection:

    """
        The purpose of this function is to connect to Snowflake using the connector (if not already connected)
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameters to run a query
        Returns:
            the snowflake connection object with which we can run query
        Raises:
            Retry 3 times and exits the program if error connecting (with decorators)
    """

    global current_snowflake_connection  # Reference the global variable

    #We get environment keys (GitHub secrets)
    SNOWFLAKE_USERNAME = os.getenv('SNOWFLAKE_USERNAME')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')

    #if we are already connected no need to do it again
    if not(current_snowflake_connection and not current_snowflake_connection.is_closed()):
        if os.getenv("IS_TESTRUN") == '0':
            database = sr_snowflake_account['DATABASE_PROD']
        else:
            database = sr_snowflake_account.at['DATABASE_TEST']

        current_snowflake_connection = snowflake.connector.connect(
            user = SNOWFLAKE_USERNAME,
            password= SNOWFLAKE_PASSWORD,
            account=sr_snowflake_account['ACCOUNT'],
            warehouse=sr_snowflake_account['WAREHOUSE'],
            database=database,
            schema=config.landing_database_schema,
            role=config.role_database,
            login_timeout=config.snowflake_login_wait_time
        )
        logging.info(f"SNOWFLAKE -> CONNECTED")
    return current_snowflake_connection

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('query','params') })
@config.retry_function(log_filter=lambda args: {k: args[k] for k in ('query','params') })
def snowflake_execute(sr_snowflake_account: pd.Series, query: str, params: Sequence[Any] | Mapping[str, Any] | None =None) -> pd.DataFrame | list | None:

    """
        The purpose of this function is to:
        - personalize a snowflake query 
        - run it
        - calculate the related dataframe if it is a select query
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to run a query
            query (str): The query we want to run
            params (list): list of parameters to personalize the query with
        Returns:
            - if the query is a select query then return the dataframe related 
            - if the query is a show query then return a list
            - else return None
        Raises:
            Retry 3 times and exits the program if error executing or parsing the query (with decorators)
    """
    
    #We connect to Snowflake
    snowConnect = snowflake_connect(sr_snowflake_account)
    
    #We personalized #DATABASE# and run the query
    if os.getenv("IS_TESTRUN") == '0':
        database = sr_snowflake_account['DATABASE_PROD']
    else:
        database = sr_snowflake_account['DATABASE_TEST']
    with snowConnect.cursor() as snowCursor:
        query_personalized = query.replace("#DATABASE#",database)
        snowCursor.execute(query_personalized,params)

        #We parse the query using sqlglot to possibly return the related dataframe/list
        query_root = sqlglot.parse_one(query_personalized.replace("%s", "NULL"), read="snowflake")
        if isinstance(query_root, exp.With):
            query_root = query_root.this

        #If it doesn't a sql keyword we raise an error
        if not hasattr(query_root, "key"):
            raise ValueError("The sql is not valid (doesn't have a keyword)")
        #If it is a select query we return the associated dataframe
        if query_root.key.upper() == "SELECT":
            df = snowCursor.fetch_pandas_all()
            return df
        #If it is a show query we return the associated list
        if query_root.key.upper() == "SHOW":
            lst = snowCursor.fetchall()
            return lst

@config.exit_program(log_filter=lambda args: {})
@config.retry_function(log_filter=lambda args: {})
def snowflake_execute_script(sr_snowflake_account: pd.Series, script: str):

    """
        The purpose of this function is to:
        - personalize "#DATABASE#" in a snowflake list of queries gathered in a script
        - run them
        Args:
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to run a query
            script (str): A set of queries gathered in a script
        Raises:
            Retry 3 times and exits the program if error executing the script (with decorators)
    """
    
    #We connect to Snowflake
    snowConnect = snowflake_connect(sr_snowflake_account)
    
    #We personalized #DATABASE# and run the query
    if os.getenv("IS_TESTRUN") == '0':
        database = sr_snowflake_account['DATABASE_PROD']
    else:
        database = sr_snowflake_account['DATABASE_TEST']
    script_personalized = script.replace("#DATABASE#",database)
    snowConnect.execute_string(script_personalized)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('called_by','message_action','game_action','calculation_needed')})
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
   
    logging.info(f"SNOWFLAKE -> LISTING TABLES TO UPDATE [START]")
    #We get the category of tables we need
    lst_python_category = []
    lst_dbt_category = []

    if called_by == config.CALLER["COMPET"]:
        lst_python_category.extend([config.DATABASE_RUN_CATEGORY_MAP["INIT_COMPET"]])
        lst_dbt_category.extend([config.DATABASE_RUN_CATEGORY_MAP["INIT_COMPET"]])
        
    elif called_by == config.CALLER["MAIN"]:
        if message_action == config.MESSAGE_ACTION_MAP['CHECK']:
            lst_python_category.extend([config.DATABASE_RUN_CATEGORY_MAP["MESSAGE_CHECK"]])
            lst_dbt_category.extend([config.DATABASE_RUN_CATEGORY_MAP["MESSAGE_CHECK"]])

        if message_action == config.MESSAGE_ACTION_MAP["RUN"]:
            lst_python_category.extend([config.DATABASE_RUN_CATEGORY_MAP["MESSAGE_RUN"]])
            lst_dbt_category.extend([config.DATABASE_RUN_CATEGORY_MAP["MESSAGE_CHECK"]])
            lst_dbt_category.extend([config.DATABASE_RUN_CATEGORY_MAP["MESSAGE_RUN"]])

        if game_action == config.GAME_ACTION_MAP["RUN"]:
            lst_python_category.extend([config.DATABASE_RUN_CATEGORY_MAP["GAME_RUN"]])
            lst_dbt_category.extend([config.DATABASE_RUN_CATEGORY_MAP["GAME_RUN"]])
            
        if (message_action==config.MESSAGE_ACTION_MAP["RUN"] and calculation_needed):
            lst_dbt_category.extend([config.DATABASE_RUN_CATEGORY_MAP["CALCULATION"]])
            
    #We get list of tables which need to be updated based on categories
    df_paths_python_exploded = df_paths.explode('PYTHON_CATEGORY')
    lst_python_tables = df_paths_python_exploded[df_paths_python_exploded['PYTHON_CATEGORY'].isin(lst_python_category)]['NAME'].unique().tolist()

    df_paths_dbt_exploded = df_paths.explode('DBT_CATEGORY')
    lst_dbt_tables = df_paths_dbt_exploded[df_paths_dbt_exploded['DBT_CATEGORY'].isin(lst_dbt_category)]['NAME'].unique().tolist()

    if called_by == config.CALLER["MAIN"]:
        #We add the landing table corresponding to output_need file
        lst_python_tables.extend(['landing_output_need'])

    # We remove possibles duplicates
    lst_python_tables = list(set(lst_python_tables))
    lst_dbt_tables = list(set(lst_dbt_tables))

    logging.info("__________________________________________________________________")
    logging.info(f"LIST OF TABLES TO BE UPDATED BY PYTHON: {lst_python_tables}")
    logging.info(f"LIST OF TABLES TO BE UPDATED BY DBT: {lst_dbt_tables}")
    logging.info("__________________________________________________________________")

    logging.info(f"SNOWFLAKE -> LISTING TABLES TO UPDATE [END]")
    return [lst_python_tables,lst_dbt_tables]

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('table_metadata',)})
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
    qDeleteData = sqlQ.snowflake_actions_qDeleteData.replace("#SCHEMA#",schema).replace("#TABLE_NAME#",table_name)
    snowflake_execute(sr_snowflake_account,qDeleteData)

    #for each table stage @%, we delete files
    qRemoveFromStage = sqlQ.snowflake_actions_qRemoveFromStage.replace("#SCHEMA#",schema).replace("#TABLE_NAME#",table_name)
    snowflake_execute(sr_snowflake_account,qRemoveFromStage)
    logging.info(f"SNOWFLAKE {table_name.upper()} -> DATA DELETED")

@config.exit_program(log_filter=lambda args: {})
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
    qListTables = sqlQ.snowflake_actions_qListTables.replace("#SCHEMA#",schema)
    lst_tables = snowflake_execute(sr_snowflake_account,qListTables)

    # We parallelize the data deletion of those tables and their stages
    table_args = [(sr_snowflake_account,schema,table_metadata) for table_metadata in lst_tables]
    config.multithreading_run(delete_table_data, table_args)

    logging.info(f"SNOWFLAKE {schema} -> DELETING DATA [DONE]")

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('table_name','is_encapsulated')})
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

    qSelectData = sqlQ.snowflake_actions_qSelectData.replace("#SCHEMA#",schema).replace("#TABLE_NAME#",table_name)
    df = snowflake_execute(sr_snowflake_account,qSelectData)

    #we create the csv file
    create_csv(os.path.join(config.TMPD,table_name)+'.csv',df,is_encapsulated)    

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('called_by','local_folder')})
def update_snowflake_from_python(called_by: str, sr_snowflake_account: pd.Series, table_name: str, df_paths: pd.DataFrame, local_folder: str):

    """
        The purpose of this function is to:
        -  update a snowflake table and its stage from a python script using an input file
            * when called by main or init_compet, the input file created by python have the name of the table, minus "landing_"
            * when called by init_snowflake, the input file has the same name, as we downloaded the table file directly from dropbox
        -  create a csv file of the updated table (only when called by main or init_compet, we already have it when called by init_snowflake)
        Args:
            called_by (str): The exe function calling this function
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
    if called_by in (config.CALLER["MAIN"],config.CALLER["COMPET"]):
        file_name = '_'.join(table_name.split('_')[1:])
    #if not, the input file is the same name
    elif called_by == config.CALLER["SNOWFLAKE"]:
        file_name = table_name

    #we get info about the file
    file_path = os.path.join(local_folder,file_name+'.csv')
    file_path_abs = Path(file_path).resolve()
    is_encapsulated = df_paths.loc[df_paths['NAME'] == table_name, 'IS_ENCAPSULATED'].iloc[0]

    #we update stage and table
    qInsertData = sqlQ.snowflake_actions_qInsertData.replace("#SCHEMA#",schema).replace("#TABLE_NAME#",table_name)
    if (is_encapsulated == 1):
         qInsertData = qInsertData.replace("#ISENCLOSED#", 
                                           "FIELD_OPTIONALLY_ENCLOSED_BY=\'\"\' NULL_IF = (\'\', \'NULL\')")
    else:
         qInsertData = qInsertData.replace("#ISENCLOSED#", "")

    qPutToStage = sqlQ.snowflake_actions_qPutToStage.replace("#FILE_PATH_ABS#",str(file_path_abs)).replace("#SCHEMA#",schema).replace("#TABLE_NAME#",table_name)
    snowflake_execute(sr_snowflake_account,qPutToStage)
    snowflake_execute(sr_snowflake_account,qInsertData)
    #if called by main or init_compet, we need to create the file from the table
    if called_by in [config.CALLER["MAIN"],config.CALLER["COMPET"]]:
        create_table_file(sr_snowflake_account, table_name, is_encapsulated)
    logging.info(f"SNOWFLAKE {table_name} -> UPDATING FROM PYTHON [DONE]")

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('called_by','lst_dbt_tables')})
def update_snowflake_from_dbt(called_by: str, sr_snowflake_account: pd.Series, df_paths: pd.DataFrame,  lst_dbt_tables: list[str] | None = None):
    
    '''
        The purpose of this function is to:
         - call dbt and run a command to update Snowflake tables, according to the "exe" function calling it
         - download results of tables updated into csv, according to the "exe" function calling it
        Inputs:
            called_by (str): The "exe" function calling this one
            sr_snowflake_account (series - one row) : Contains the snowflake account parameter to download tables into csv
            df_paths (dataframe): containing details about tables to download them
        Raises:
            If there is an error running dbt we directly exit the program with decorator
    '''
    
    logging.info("SNOWFLAKE -> UPDATING TABLES FROM DBT [START]")

    if called_by == config.CALLER["SNOWFLAKE"]:
        # we just run seeds and views
        dbt_command = "dbt build --select config.materialized:seed + config.materialized:view --exclude test_type:unit --fail-fast"
        bl_select_table = False
    else:
        # we run all tables from the list
        dbt_command = "dbt build --select source:* +"  + " ".join(lst_dbt_tables) + " --exclude test_type:unit --fail-fast"
        bl_select_table = True
    
    # Set environment variable for DBT profiles dir
    os.environ["DBT_PROFILES_DIR"] = config.dbt_directory

    # Common parameters for subprocess.run
    run_params = {
        "cwd": config.dbt_directory,
        "shell": True,
        "text": True,
        "capture_output": True,
        "env": os.environ.copy(),
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

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('called_by','local_folder')})
def update_snowflake(called_by: str, context_dict: dict, local_folder: str):

    """
        The purpose of this function is to:
        -  get the list of snowflake objects that needs to be run, according to the "exe" function calling it
        -  run them either from python or dbt
        Args:
            called_by (str) : The exe function calling this one
            context_dict (data dictionary): The data dictionary containing main run objects:
                some of them are used to get the list of snowflake objects to update
            local_folder (str): The local folder where we download the files related to updated tables
        Raises:
            Exits the program if error running the function (using decorator)
    """

    df_paths = context_dict['df_paths']
    sr_snowflake_account = context_dict['sr_snowflake_account_connect']
    
    #we get the list of table we should update - except if called_by init_snowflake: we don't need it
    if called_by == config.CALLER["MAIN"]:
        message_action = context_dict['sr_output_need']['MESSAGE_ACTION']
        game_action = context_dict['sr_output_need']['GAME_ACTION']
        calculation_needed = (context_dict['sr_output_need']['IS_TO_CALCULATE'] + context_dict['sr_output_need']['IS_TO_DELETE'] + context_dict['sr_output_need']['IS_TO_RECALCULATE'] > 0)
        [lst_python_tables,lst_dbt_tables] = get_list_tables_to_update(called_by,df_paths,message_action,game_action,calculation_needed)
    elif called_by == config.CALLER["COMPET"]:
        [lst_python_tables,lst_dbt_tables] = get_list_tables_to_update(called_by,df_paths)
    
    # we update tables from python from lst_python_tables 
    # and dbt with lst_dbt_tables - except for init_snowflake
    if called_by in (config.CALLER["MAIN"],config.CALLER["COMPET"]):
        
        if len(lst_python_tables) != 0:
            
            table_args = [(called_by, 
                        sr_snowflake_account,
                        table_name,
                        df_paths,
                        local_folder) for table_name in lst_python_tables]
            config.multithreading_run(update_snowflake_from_python, table_args)
        
        if len(lst_dbt_tables) != 0:
            update_snowflake_from_dbt(called_by, sr_snowflake_account, df_paths, lst_dbt_tables) 
        
    # in this case we do it from python with the list of downloaded table files
    elif called_by == config.CALLER["SNOWFLAKE"]:
        file_args = [(called_by, 
                sr_snowflake_account,
                Path(file).stem,
                df_paths,
                local_folder) for file in os.listdir(local_folder)]
        config.multithreading_run(update_snowflake_from_python, file_args)

        # no direct tables to update from dbt as we just copied all data from the files into the related tables
        # we call dbt to create seeds and views
        update_snowflake_from_dbt(called_by,sr_snowflake_account,df_paths)