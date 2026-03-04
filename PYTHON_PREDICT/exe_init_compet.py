'''
This module is an entry point of the program, it runs the exe_init_compet function
'''
import logging
logging.basicConfig(level=logging.INFO)
import json

import config
import file_actions.file_actions as fileA
import game_actions.game_actions as gameA
import snowflake_actions as snowflakeA
from dropbox_actions import initiate_folder as dropbox_initiate_folder
from calendar_actions import update_calendar_related_files

@config.exit_program(log_filter=lambda args: {})
def exe_init_compet():

    '''
        This "exe" function can be called directly by the user or GitHub action
        Its purpose is to add new competition to the database, with all games related.
        It:
        - downloads input files related to those competition locally from DropBox
        - extracts games details related to a given competition
        - updates database with those new games via python and dbt sql scripts, and extracts table result in csv files
        - uploads them to DropBox
        
    '''
    logging.info("EXE INIT COMPET -> START")
    called_by = config.CALLER["COMPET"]
    context_dict = {}
    
    # We create the environment to work with dropbox and local files, 
    # and download initial files we will need for process
    dropbox_initiate_folder()
    context_dict.update(fileA.initiate_local_environment(called_by))

    # we get games infos related to the competition
    # by first filtering which game are not yet in the database
    context_dict['df_game'] = gameA.extract_games_from_competition(context_dict['df_competition'])

    # we filter input files, to get only input related to those games   
    context_dict.update(fileA.filter_data(files_data_dict = context_dict, 
                                df_paths=context_dict['df_paths'], 
                                filtering_category = config.game_filtering_category))

    # We update tables in snowflake, by first removing everything on the landing (first) layer
    snowflakeA.delete_tables_data_from_python(context_dict['sr_snowflake_account_connect'], schema=config.landing_database_schema)
    snowflakeA.update_snowflake(called_by,context_dict,config.TMPF)

    # The new added games may have change the calendar of run, we update its file    
    context_dict['str_next_run_time_utc'] = update_calendar_related_files(
        called_by,
        context_dict['sr_snowflake_account_connect'],
        context_dict['df_task_done'])
    
    # we finally terminate the local_environment uploading files to DropBox and destroying local folders created
    fileA.terminate_local_environment(called_by,context_dict)

    with open("exe_output.json", "w") as f:
        json.dump({
            "next_run": context_dict['str_next_run_time_utc']
        }, f) 

    logging.info("EXE INIT COMPET -> END")

if __name__ == "__main__":
    exe_init_compet()