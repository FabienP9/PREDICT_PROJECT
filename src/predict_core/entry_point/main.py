'''
This module is the main entry point of the program, it runs the main function, which runs a task.
'''
import logging
import os

from ..config import config_decorators
from ..config.config_multithread import multithread_run
from ..config.config_variables import config_global_variables as var
from ..config.config_variables import config_environment_variables as env
from ..files_manipulation.external_files_interaction import dropbox_files_interaction as dropbox
from ..files_manipulation.local_files_manipulation import local_environment_manipulation
from ..files_manipulation.local_files_manipulation import files_manipulation
from ..files_manipulation.local_files_manipulation.specific_files_operations.output_message_file_generation import output_message_generation
from ..files_manipulation.local_files_manipulation.specific_files_operations.specific_files_operations import create_json_file_email
from ..tasks_management import output_need_calculation
from ..tasks_management import tasks_calendar_management
from ..games_details_extraction import games_details_extraction
from ..forums_interaction import messages_details_extraction
from ..forums_interaction import messages_posting_process
from ..database_interaction.snowflake_etl_process import snowflake_etl_process

logging.basicConfig(level=logging.INFO)

@config_decorators.exit_program(log_filter=lambda args: {})
def process_games(context_dict: dict) -> dict:

    '''
        Process games for main function
        Args:
            context_dict (dict) : Contains all details processed by main function
        Returns:
            dict: context dict updated with the games processed
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    context_dict['df_game'] = games_details_extraction.extract_games_from_need(context_dict['sr_output_need'],context_dict['df_competition'],context_dict['df_gameday_modification'])
    
    # we filter game files, to get only inputs related to those games   
    context_dict.update(files_manipulation.filter_data(files_data_dict = context_dict, df_paths=context_dict['df_paths'], filtering_category = var.GAME_FILTERING_CATEGORY))
    return context_dict

@config_decorators.exit_program(log_filter=lambda args: {})
def process_messages(context_dict: dict) -> dict:

        '''
            Process messages for main function
            Args:
                context_dict (dict) : Contains all details processed by main function
            Returns:
                dict: context dict updated with the messages processed
            Raises:
                Exits the program if error running the function (using decorator)
        '''

        # We want to extract messages from the forum and download the input files related                     
        context_dict['df_message_check'],context_dict['extraction_time_utc']= \
            messages_details_extraction.extract_messages(context_dict['sr_snowflake_account_connect'],
                                                         context_dict['sr_output_need'])            

        # we filter messages files, to get only inputs related to those messages   
        context_dict.update(files_manipulation.filter_data(files_data_dict = context_dict, 
                                                           df_paths=context_dict['df_paths'], 
                                                           filtering_category = var.MESSAGE_FILTERING_CATEGORY))        

        # We count number of new messages -which are not beginning with 5+ (posted by the program) or 5* (technical)
        messages = context_dict['df_message_check']
        context_dict['nb_new_messages'] = messages[~messages['MESSAGE_CONTENT'].str.startswith(var.MESSAGE_PREFIX_PROGRAM_STRING, 
                                                                                               var.MESSAGE_PREFIX_TECHNICAL_STRING)].shape[0]
        
        bl_check_message = context_dict['df_boolean_check_message_manually'].loc[context_dict['df_boolean_check_message_manually']['SEASON_ID']\
                                                         == context_dict['sr_output_need']['SEASON_ID'], 'BOOLEAN_CHECK_MESSAGE_MANUALLY'].iloc[0]
        if (context_dict['nb_new_messages'] > 0) and (bl_check_message == 1):
        
            # if there are new messages, and check_message is required, and we were supposed to run, we modify the output_need file and the related dataframe
            if context_dict['sr_output_need']['MESSAGE_ACTION'] == var.MESSAGE_ACTION_MAP["RUN"]:

                output_need_calculation.set_output_need_to_check_status(context_dict['sr_output_need'])
        else:
            # We copy the message_check file to a file message, with encapsulation
            context_dict['df_message'] = context_dict['df_message_check']
            files_manipulation.create_csv(os.path.join(var.TMPF,'message.csv'),context_dict['df_message'],
                                          var.MESSAGE_FILE_ENCAPSULATED) 

            # We modify message_check_ts with extraction time
            context_dict['df_message_check_ts'].loc[context_dict['df_message_check_ts']['SEASON_ID']\
                             == context_dict['sr_output_need']['SEASON_ID'], 'LAST_CHECK_TS_UTC'] = context_dict['extraction_time_utc'] 
            files_manipulation.create_csv(os.path.join(var.TMPF,'message_check_ts.csv'),context_dict['df_message_check_ts']) 

        return context_dict

@config_decorators.exit_program(log_filter=lambda args: {})
def display_check_string(context_dict: dict) -> str:

    '''
        Create check string for main function output and display it
        Args:
            context_dict (dict) : Contains all details processed by main function
        Returns:
            str: check_string
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    check_string = ""
    bl_check_message = context_dict['df_boolean_check_message_manually'].loc[context_dict['df_boolean_check_message_manually']['SEASON_ID'] == context_dict['sr_output_need']['SEASON_ID'], 'BOOLEAN_CHECK_MESSAGE_MANUALLY'].iloc[0]
    
    if context_dict['sr_output_need']['TASK_RUN'] == var.MESSAGE_ACTION_MAP['CHECK']:
        if (context_dict['nb_new_messages'] > 0) and (bl_check_message == 1):
            check_string = (f"check messages at ==> \n"
                f";SELECT * FROM {context_dict['sr_snowflake_account_connect']['DATABASE_PROD']}.CURATED.VW_MESSAGE_CHECKING WHERE SEASON_ID = '{context_dict['sr_output_need']['SEASON_ID']}' AND EDITION_TIME_UTC between '{context_dict['sr_output_need']['LAST_MESSAGE_CHECK_TS_UTC']}' AND '{context_dict['extraction_time_utc']}'; \n"
                f"If ok replace SEASON_ID {context_dict['sr_output_need']['SEASON_ID']} check time with:\n"
                f"{context_dict['extraction_time_utc']}\n")
        elif (context_dict['nb_new_messages'] == 0) and (bl_check_message == 1):
            check_string = "No need to check - no new messages"
        else:
            check_string = "No need to check - messages processed automatically"

        logging.info("__________________________________________________________________")
        logging.info(check_string)
        logging.info("__________________________________________________________________")
        return check_string

@config_decorators.exit_program(log_filter=lambda args: {})
def main():
    
    '''
    
        This entry point function can be called directly by the user or GitHub action
        Its purpose is to run the all tasks of the program
        calling functions according to the need and updating tables/views
        for eventually post message on forums
        It:
        - downloads input files locally from DropBox
        - generate the output need
        - updates snowflake database according to the output need
        - post message on forums
        
    '''

    logging.info("MAIN -> START")
    called_by = var.CALLER["MAIN"]
    env.check_environment_variable(called_by)
    context_dict = {}

    #We create the environment to work with dropbox and local files, and download initial files we will need for process
    dropbox.initiate_folder()
    context_dict.update(local_environment_manipulation.initiate_local_environment(called_by))
    
    #We create the output_need file - The next algorithm of run depends on its values
    context_dict['sr_output_need'] = output_need_calculation.generate_output_need(context_dict)
    
    # we download additional files according to the value of MESSAGE_ACTION and GAME_ACTION
    context_dict.update(dropbox.download_needed_files(context_dict['df_paths'], context_dict['sr_output_need']))
    
    if context_dict['sr_output_need']['GAME_ACTION']  == var.GAME_ACTION_MAP['RUN']:
        context_dict = process_games(context_dict)
    
    if ( context_dict['sr_output_need']['MESSAGE_ACTION'] in (var.MESSAGE_ACTION_MAP["RUN"],var.MESSAGE_ACTION_MAP["CHECK"])):
        context_dict = process_messages(context_dict)
    
    #if either messages or games are running
    if ( context_dict['sr_output_need']['MESSAGE_ACTION'] in (var.MESSAGE_ACTION_MAP["RUN"],var.MESSAGE_ACTION_MAP["CHECK"]) 
        or context_dict['sr_output_need']['GAME_ACTION'] == var.GAME_ACTION_MAP["RUN"]):
        
        # We update tables in snowflake
        snowflake_etl_process.delete_tables_data_from_python(context_dict['sr_snowflake_account_connect'],"LANDING")
        snowflake_etl_process.update_snowflake(called_by,context_dict, var.TMPF)
    
    # The new added games or just ran task may have change the calendar of run
    context_dict['str_next_run_time_utc'] = tasks_calendar_management.update_calendar_related_files(called_by, context_dict['sr_snowflake_account_connect'], context_dict['df_task_done'], context_dict['sr_output_need'])

    if context_dict['sr_output_need']['TASK_RUN'] in (var.TASK_RUN_MAP["INIT"],var.TASK_RUN_MAP["CALCULATE"]):
        output_param_dict, df_topics = output_message_generation.generate_output_message(context_dict)

        # we post messages for each concerned topics
        posting_args = [(df_topics.iloc[i].to_dict(), 
                         output_param_dict[f"MESSAGE_{df_topics.iloc[i]['FORUM_COUNTRY']}_{df_topics.iloc[i]['FORUM_SOURCE']}"])
            for i in range(len(df_topics))]   
        multithread_run(messages_posting_process.post_message, posting_args)
    
    local_environment_manipulation.terminate_local_environment(called_by,context_dict)
    
    str_output_need = "\n".join(f"{idx} = {context_dict['sr_output_need'][idx]}" for idx in context_dict['sr_output_need'].index)
    check_string = display_check_string(context_dict)
    create_json_file_email(str_next_run_time_utc = context_dict['str_next_run_time_utc'],
                           str_output_need = str_output_need,
                           check_string = check_string)

    logging.info("MAIN -> DONE")
    
if __name__ == "__main__":
    main()