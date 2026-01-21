'''
This module is the main entry point of the program, it runs the exe_main function
'''
import logging
logging.basicConfig(level=logging.INFO)
import json
import os
import pandas as pd

import config
import file_actions as fileA
from dropbox_actions import initiate_folder as dropbox_initiate_folder
from calendar_actions import update_calendar_related_files
from generate_output_need import generate_output_need
from generate_output_need import set_output_need_to_check_status
import game_actions as gameA
from message_actions import extract_messages
import snowflake_actions as snowflakeA
from output_actions import output_actions as outputA

@config.exit_program(log_filter=lambda args: {})
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

    context_dict['df_game'] = gameA.extract_games_from_need(context_dict['sr_output_need'],context_dict['df_competition'])
    
    # we filter game files, to get only inputs related to those games   
    context_dict.update(fileA.filter_data(files_data_dict = context_dict, df_paths=context_dict['df_paths'], filtering_category = config.game_filtering_category))
    return context_dict

@config.exit_program(log_filter=lambda args: {})
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
        context_dict['df_message_check'],context_dict['extraction_time_utc'] = extract_messages(context_dict['sr_snowflake_account_connect'],context_dict['sr_output_need'])            

        # we filter messages files, to get only inputs related to those messages   
        context_dict.update(fileA.filter_data(files_data_dict = context_dict, df_paths=context_dict['df_paths'], filtering_category = config.message_filtering_category))        

        # We count number of new messages -which are not beginning with 5+ (posted by the program) or 5* (technical)
        messages = context_dict['df_message_check']
        context_dict['nb_new_messages'] = messages[~messages['MESSAGE_CONTENT'].str.startswith(config.message_prefix_program_string, config.message_prefix_technical_string)].shape[0]
        
        bl_check_message = context_dict['df_boolean_check_message_manually'].loc[context_dict['df_boolean_check_message_manually']['SEASON_ID'] == context_dict['sr_output_need']['SEASON_ID'], 'BOOLEAN_CHECK_MESSAGE_MANUALLY'].iloc[0]
        if (context_dict['nb_new_messages'] > 0) and (bl_check_message == 1):
        
            # if there are new messages, and check_message is required, and we were supposed to run, we modify the output_need file and the related dataframe
            if context_dict['sr_output_need']['MESSAGE_ACTION'] == config.MESSAGE_ACTION_MAP["RUN"]:

                set_output_need_to_check_status(context_dict['sr_output_need'])
        else:
            # We copy the message_check file to a file message, with encapsulation
            context_dict['df_message'] = context_dict['df_message_check']
            fileA.create_csv(os.path.join(config.TMPF,'message.csv'),context_dict['df_message'],config.message_encapsulated) 

            # We modify message_check_ts with extraction time
            context_dict['df_message_check_ts'].loc[context_dict['df_message_check_ts']['SEASON_ID'] == context_dict['sr_output_need']['SEASON_ID'], 'LAST_CHECK_TS_UTC'] = context_dict['extraction_time_utc'] 
            fileA.create_csv(os.path.join(config.TMPF,'message_check_ts.csv'),context_dict['df_message_check_ts']) 


        return context_dict

@config.exit_program(log_filter=lambda args: {})
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
    
    if context_dict['sr_output_need']['TASK_RUN'] == config.MESSAGE_ACTION_MAP['CHECK']:
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
    
@config.exit_program(log_filter=lambda args: {})
def exe_main():
    
    '''
    
        This "exe" function can be called directly by the user or GitHub action
        Its purpose is to run the all tasks of the program
        calling functions according to the need and updating tables/views
        for eventually post message on forums
        It:
        - downloads input files locally from DropBox
        - generate the output need
        - updates snowflake database according to the output need
        - post message on forums
        
    '''

    logging.info("EXE -> MAIN [START]")
    called_by = config.CALLER["MAIN"]
    context_dict = {}

    #We create the environment to work with dropbox and local files, and download initial files we will need for process
    dropbox_initiate_folder()
    context_dict.update(fileA.initiate_local_environment(called_by))
    
    #We create the output_need file - The next algorithm of run depends on its values
    context_dict['sr_output_need'] = generate_output_need(context_dict)
    
    # we download additional files according to the value of MESSAGE_ACTION and GAME_ACTION
    context_dict.update(fileA.download_needed_files(context_dict['df_paths'], context_dict['sr_output_need']))
    
    if context_dict['sr_output_need']['GAME_ACTION']  == config.GAME_ACTION_MAP['RUN']:
        context_dict = process_games(context_dict)
    
    if ( context_dict['sr_output_need']['MESSAGE_ACTION'] in (config.MESSAGE_ACTION_MAP["RUN"],config.MESSAGE_ACTION_MAP["CHECK"])):
        context_dict = process_messages(context_dict)
    
    if ( context_dict['sr_output_need']['MESSAGE_ACTION'] in (config.MESSAGE_ACTION_MAP["RUN"],config.MESSAGE_ACTION_MAP["CHECK"]) or context_dict['sr_output_need']['GAME_ACTION'] == config.GAME_ACTION_MAP["RUN"]):
        snowflakeA.delete_tables_data_from_python(context_dict['sr_snowflake_account_connect'],"LANDING")

        # We update tables in snowflake
        snowflakeA.update_snowflake(called_by,context_dict, config.TMPF)
    
        # The new added games may have change the calendar of run, we update its file    
        context_dict['str_next_run_time_utc'] = update_calendar_related_files(called_by, context_dict['sr_snowflake_account_connect'], context_dict['df_task_done'], context_dict['sr_output_need'])
        
    if context_dict['sr_output_need']['TASK_RUN'] in (config.TASK_RUN_MAP["INIT"],config.TASK_RUN_MAP["CALCULATE"]):
        outputA.generate_output_message(context_dict)
    
    fileA.terminate_local_environment(called_by,context_dict)

    check_string = display_check_string(context_dict)
    str_output_need = "\n".join(f"{idx} = {context_dict['sr_output_need'][idx]}" for idx in context_dict['sr_output_need'].index)

    with open("exe_output.json", "w") as f:
        json.dump({
            "next_run": context_dict['str_next_run_time_utc'],
            "str_output_need": str_output_need,
            "check_string": check_string
        }, f) 
    logging.info("EXE -> MAIN [DONE]")
    
if __name__ == "__main__":
    exe_main()