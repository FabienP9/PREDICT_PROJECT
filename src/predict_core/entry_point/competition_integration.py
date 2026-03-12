'''
This module is an entry point of the program, it runs the competition_integration function
to integrate a new competition into the databse
'''
import logging

from ..config import config_decorators
from ..config.config_variables import config_global_variables as var
from ..config.config_variables import config_environment_variables as env
from ..files_manipulation.external_files_interaction import dropbox_files_interaction as dropbox
from ..files_manipulation.local_files_manipulation import local_environment_manipulation
from ..files_manipulation.local_files_manipulation import files_manipulation
from ..files_manipulation.local_files_manipulation.specific_files_operations.specific_files_operations import create_json_file_email
from ..games_details_extraction import games_details_extraction
from ..database_interaction.snowflake_etl_process import snowflake_etl_process
from ..tasks_management.tasks_calendar_management import update_calendar_related_files

logging.basicConfig(level=logging.INFO)

@config_decorators.exit_program(log_filter=lambda args: {})
def competition_integration():

    '''
        This entry point function can be called directly by the user or GitHub action
        Its purpose is to add new competition to the database, with all games related.
        It:
        - downloads input files related to those competition locally from DropBox
        - extracts games details related to a given competition
        - updates database with those new games via python and dbt sql scripts, and extracts table result in csv files
        - uploads them to DropBox
        
    '''
    logging.info("INIT COMPET -> START")
    called_by = var.CALLER["COMPET"]
    env.check_environment_variable(called_by)
    context_dict = {}
    
    # We create the environment to work with dropbox and local files, 
    # and download initial files we will need for process
    dropbox.initiate_folder()
    context_dict.update(local_environment_manipulation.initiate_local_environment(called_by))

    # we get games infos related to the competition
    # by first filtering which game are not yet in the database
    context_dict['df_game'] = games_details_extraction.extract_games_from_competition(context_dict['df_competition'])

    # we filter input files, to get only input related to those games   
    context_dict.update(files_manipulation.filter_data(files_data_dict = context_dict, 
                                df_paths=context_dict['df_paths'], 
                                filtering_category = var.GAME_FILTERING_CATEGORY))

    # We update tables in snowflake, by first removing everything on the landing (first) layer
    snowflake_etl_process.delete_tables_data_from_python(context_dict['sr_snowflake_account_connect'], schema=var.LANDING_DATABASE_SCHEMA)
    snowflake_etl_process.update_snowflake(called_by,context_dict,var.TMPF)

    # The new added games may have change the calendar of run, we update its file    
    context_dict['str_next_run_time_utc'] = update_calendar_related_files(
        called_by,
        context_dict['sr_snowflake_account_connect'],
        context_dict['df_task_done'])
    
    # we finally terminate the local_environment uploading files to DropBox and destroying local folders created
    local_environment_manipulation.terminate_local_environment(called_by,context_dict)

    create_json_file_email(str_next_run_time_utc = context_dict['str_next_run_time_utc'])

    logging.info("INIT COMPET -> END")

if __name__ == "__main__":
    competition_integration()