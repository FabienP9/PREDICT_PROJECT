'''
This module is an entry point of the program, it runs the snowflake_account_initialization function,
to insert all data into a new snowflake account
'''
import logging

from ..config import config_decorators
from ..config.config_variables import config_global_variables as var
from ..config.config_variables import config_environment_variables as env
from ..files_manipulation.local_files_manipulation import local_environment_manipulation
from ..files_manipulation.external_files_interaction import dropbox_files_interaction as dropbox
from ..database_interaction import snowflake_connection_execution
from ..database_interaction.snowflake_etl_process import sql_queries
from ..database_interaction.snowflake_etl_process import snowflake_etl_process

logging.basicConfig(level=logging.INFO)


@config_decorators.exit_program(log_filter=lambda args: {})
def snowflake_account_initialization():

    '''
        This function can be called directly by the user or GitHub action
        Its purpose is to create a new database on a snowflake account
        with tables/views filled with last run data
        It:
        - downloads input files and database files locally from DropBox
        - initializes the database with the "script_creating_database" file
        - updates snowflake database with database files, and create seeds and views
        
    '''
    logging.info("INIT SNOWFLAKE -> START")
    called_by = var.CALLER["SNOWFLAKE"]
    env.check_environment_variable(called_by)
    context_dict = {}

    # We create the environment to work with dropbox and local files, and download initial files we will need for process
    dropbox.initiate_folder()
    context_dict.update(local_environment_manipulation.initiate_local_environment(called_by))
  
    # We create the database with the script 
    snowflake_connection_execution.snowflake_execute_script(context_dict['sr_snowflake_account_connect'],context_dict['str_script_creating_database'], sql_queries.DATABASE)
    logging.info("INIT SNOWFLAKE -> DATABASE INITIALIZED")

    # We download all files from dropbox database folder 
    dropbox.download_folder("database_folder",context_dict['df_paths'],var.TMPD)

    # We update snowflake tables with data from files and create seeds and views
    snowflake_etl_process.update_snowflake(called_by,context_dict,var.TMPD)
        
    # we finally terminate the local_environment uploading the log of the run to DropBox and we destroy local folders created
    local_environment_manipulation.terminate_local_environment(called_by,context_dict)
  
    logging.info("INIT SNOWFLAKE -> END")

if __name__ == "__main__":
    snowflake_account_initialization()

