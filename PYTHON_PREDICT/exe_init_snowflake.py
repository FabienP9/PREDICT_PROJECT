'''
This module is an entry point of the program, it runs the exe_init_snowflake function
'''
import logging
logging.basicConfig(level=logging.INFO)

import config
import dropbox_actions as dropboxA
import file_actions.file_actions as fileA
import snowflake_actions as snowflakeA

@config.exit_program(log_filter=lambda args: {})
def exe_init_snowflake():

    '''
        This "exe" function can be called directly by the user or GitHub action
        Its purpose is to create a new database on a snowflake account
        with tables/views filled with last run data
        It:
        - downloads input files and database files locally from DropBox
        - initializes the database with the "script_creating_database" file
        - updates snowflake database with database files, and create seeds and views
        
    '''
    logging.info("EXE INIT SNOWFLAKE -> START")
    called_by = config.CALLER["SNOWFLAKE"]
    context_dict = {}

    # We create the environment to work with dropbox and local files, and download initial files we will need for process
    dropboxA.initiate_folder()
    context_dict.update(fileA.initiate_local_environment(called_by))
  
    # We create the database with the script 
    snowflakeA.snowflake_execute_script(context_dict['sr_snowflake_account_connect'],context_dict['str_script_creating_database'])
    logging.info("EXE INIT SNOWFLAKE -> DATABASE INITIALIZED")

    # We download all files from dropbox database folder 
    dropboxA.download_folder("database_folder",context_dict['df_paths'],config.TMPD)

    #We update snowflake tables with data from files and create seeds and views
    snowflakeA.update_snowflake(called_by,context_dict,config.TMPD)
        
    # we finally terminate the local_environment uploading the log of the run to DropBox and we destroy local folders created
    fileA.terminate_local_environment(called_by,context_dict)
  
    logging.info("EXE INIT SNOWFLAKE -> END")

if __name__ == "__main__":
    exe_init_snowflake()

