'''
    The purpose of this module is to generate message personalized for an inited gameday on forums topics.
    It gets the template message, and replace all parameters with calculated ones by connectiong to snowflake database.
    Parameters being translated will be first encoded as
    - __L__xxx__L__ for text being translated in local language
    - __F__xxx__F__ for text being translating as specific forum layout
'''
import logging
logging.basicConfig(level=logging.INFO)
import os

from datetime import datetime, timezone
import pandas as pd
from typing import Tuple

from snowflake_actions import snowflake_execute
import config
from message_actions import post_message
import file_actions as fileA
from output_actions import output_actions as outputA
from output_actions import output_actions_sql_queries as sqlQ

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_init',) })
def get_inited_list_games(sr_snowflake_account: pd.Series, sr_gameday_output_init: pd.Series) -> str:

    '''
        Gets the list of games to display on the output inited gameday message
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql game query
            sr_gameday_output_init (series - one row) containing the query filters
        Returns:
            A multiple row string displaying the list of games
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    
    #we extract the list of games to display, concatenating fields into a string
    df_games = snowflake_execute(sr_snowflake_account,sqlQ.qGame,(sr_gameday_output_init['SEASON_ID'],sr_gameday_output_init['GAMEDAY']))
    df_games['STRING'] = ("#" + df_games['GAME_MESSAGE'] + "# " +
                                df_games['TEAM_HOME_NAME'] + " vs " +
                                df_games['TEAM_AWAY_NAME'] + " ==> __F__italicbegin__F__ +1 __F__italicend__F__")
    # we create the LIST_GAMES string by concatenating all games-strings on several lines
    LIST_GAMES = "\n".join(df_games['STRING'])
    
    return LIST_GAMES,len(df_games)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_init',) })
def get_inited_remaining_games(sr_snowflake_account: pd.Series, sr_gameday_output_init: pd.Series) -> Tuple[str,str,int]:

    '''
        Gets the list of remaining games 
        (different from the gameday we output) to display on the output message
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql game query
            sr_gameday_output_init (series - one row) containing the query filters
        Returns:
            - A string displaying the list of gameday coming from the games remaining
            - A multiple row string displaying the list of games remaining
            - the number of games remaining contained in this list
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    defined_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    df_games_remaining = snowflake_execute(sr_snowflake_account,sqlQ.qGame_Remaining_AtDate,(defined_date,
                                                                                 sr_gameday_output_init['SEASON_ID'],
                                                                                 sr_gameday_output_init['GAMEDAY'],
                                                                                 defined_date,defined_date,defined_date))
    #we get unique gamedays from games and concatenate on one row string
    REMAINING_GAMEDAYS = " , ".join(df_games_remaining['GAMEDAY'].unique())
    df_games_remaining['STRING'] = ("#" + df_games_remaining['GAME_MESSAGE'] + "# " +
                                          df_games_remaining['TEAM_HOME_NAME'] + " vs " +
                                          df_games_remaining['TEAM_AWAY_NAME'] + " ==> __F__italicbegin__F__ +1 __F__italicend__F__")
    # we create the REMAINING_GAMES string by concatenating all games on several lines, grouped by gameday separated by bonus choices
    remaining_lines = []
    for gameday_message, group in df_games_remaining.groupby("GAMEDAY_MESSAGE", sort=False):
        remaining_lines.extend(group['STRING'])
        remaining_lines.append(f"#{gameday_message}.BN# __L__Bonus game ID__L__ ==> __F__italicbegin__F__{group.iloc[0]["GAME_MESSAGE"]}__F__italicend__F__")
    REMAINING_GAMES = "\n".join(remaining_lines)
    return REMAINING_GAMEDAYS,REMAINING_GAMES,len(df_games_remaining)

@config.exit_program(log_filter=lambda args: dict(args))
def get_inited_dategame1(sr_gameday_output_init: pd.Series) -> str:
    
    '''
        Gets the datetime of the first game of a given gameday into a formatted string
        Inputs:
            sr_gameday_output_init (series - one row) containing the date and time of first game
        Returns:
            The string presenting the datetime like this: "day_of_the_week DD/MM HHh:MM", MM being optional if it's 00
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    begin_date_weekday = sr_gameday_output_init['BEGIN_DATE_WEEKDAY']
    begin_date_local = sr_gameday_output_init['BEGIN_DATE_LOCAL'].strftime("%d/%m")

    # we transform HH:MM:SS to XhY (only if Y > 0)
    # example 20:15:00 becomes 20h15 / 20:00:00 becomes 20h
    begin_time_local = sr_gameday_output_init['BEGIN_TIME_LOCAL']
    h, m, _ = map(int, str(begin_time_local).split(":"))
    time_str = f"{h}h"
    if m > 0:
        time_str += f"{m}"

    DATEGAME1 = f"__L__{begin_date_weekday}__L__ {begin_date_local} {time_str}"
    return DATEGAME1

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_init',) })
def get_inited_parameters(sr_snowflake_account: pd.Series, sr_gameday_output_init: pd.Series) -> dict:

    '''
        Defines all parameters for inited gameday message
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials used in subfunction to run query
            sr_gameday_output_init (series - one row) containing parameters
        Returns:
            data dictionary with all inited parameters
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    param_dict = {}
    param_dict['GAMEDAY'] = sr_gameday_output_init['GAMEDAY']
    param_dict['LIST_GAMES'],param_dict['NB_GAMES'] = get_inited_list_games(sr_snowflake_account,sr_gameday_output_init)
    param_dict['BONUS_GAME'] = f"#{sr_gameday_output_init['GAMEDAY_MESSAGE']}.BN# __L__Bonus game ID__L__ ==> __F__italicbegin__F__{sr_gameday_output_init['GAMEDAY_MESSAGE']}.01__F__italicend__F__"
    param_dict['USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP'] = sr_gameday_output_init['USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP']
    
    REMAINING_GAMEDAYS, REMAINING_GAMES,NB_GAMES_REMAINING = get_inited_remaining_games(sr_snowflake_account,sr_gameday_output_init)
    param_dict['REMAINING_GAMEDAYS'] = REMAINING_GAMEDAYS
    param_dict['REMAINING_GAMES'] = REMAINING_GAMES
    param_dict['NB_GAMES_REMAINING'] = NB_GAMES_REMAINING
    param_dict['DATEGAME1'] = get_inited_dategame1(sr_gameday_output_init)

    return param_dict

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('country','forum','sr_gameday_output_init') })
def create_inited_message(param_dict: dict, template: str, country: str, forum:str, sr_gameday_output_init: pd.Series) -> Tuple[str,str,str]:

    '''
        Defines inited gameday message:
        - by replacing text with calculated parameters
        - create the text file containing the message, per country and per forum
        Inputs:
            param_dict (data dictionary) containing parameters
            template (str): the message text we want to personalize
            country (str): the country of the forum for the message, we translate __L__ text for this country
            forum (str): the forum for the message, we translate __F__ text for this forum formats
            sr_gameday_output_init (series - one row) containing parameters for the file name
        Returns:
            the message personalized with the related country and forum
        Raises:
            Exits the program if error running the function (using decorator)
    ''' 

    # we replace |N| in the text with N*newlines
    content = outputA.format_message(template)

    # we replace parameters systematically...
    content = content.replace("#MESSAGE_PREFIX_PROGRAM_STRING#",config.message_prefix_program_string)
    content = content.replace("#DATEGAME1#",param_dict['DATEGAME1'])  
    content = content.replace("#GAMEDAY#",param_dict['GAMEDAY'])  
    content = content.replace("#LIST_GAMES#",param_dict['LIST_GAMES'])    
    content = content.replace("#BONUS_GAME#",param_dict['BONUS_GAME'])

    # ... and on condition
    content = outputA.replace_conditionally_message(content, 
                                            "#USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP_BEGIN#", 
                                            "#USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP_END#", 
                                            param_dict['USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP'] == 1)
    
    content = outputA.replace_conditionally_message(content, 
                                                "#REMAINING_GAMES_BEGIN#", 
                                                "#REMAINING_GAMES_END#", 
                                                param_dict['NB_GAMES_REMAINING'] > 0)   
    
    if param_dict['NB_GAMES_REMAINING'] > 0: 
        content = content.replace("#REMAINING_GAMEDAYS#",param_dict['REMAINING_GAMEDAYS'])
        content = content.replace("#REMAINING_GAMES#",param_dict['REMAINING_GAMES'])

    #We then translate the content for the country and the forum
    content = outputA.translate_string(content,country,forum)

    #We finally create a filename related to this content
    file_name = outputA.define_filename("forumoutput_inited", sr_gameday_output_init, 'txt', country, forum)
    fileA.create_txt(os.path.join(config.TMPF,file_name),content)

    return content,country,forum

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_init',) })
def process_output_message_inited(context_dict: dict, sr_gameday_output_init: pd.Series):

    '''
        Defines inited gameday message:
        - by getting templates of message for each country we want to post
        - modify templates with parameters calculated
        - posting the text on forums 
        Inputs:
            context_dict (data dictionary) containing data to calculate the parameters
            sr_gameday_output_init (series - one row) containing details to calculate the parameters
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    logging.info(f"OUTPUT -> GENERATING INIT MESSAGE [START]")

    sr_snowflake_account = context_dict['sr_snowflake_account_connect']
    # we get the distinct list of topics where we want to post, and the list of distinct countries for these topics
    df_topics = snowflake_execute(sr_snowflake_account,sqlQ.qTopics_Init,(sr_gameday_output_init['SEASON_ID'],))
    list_countries_forums = (df_topics[['FORUM_COUNTRY', 'FORUM_SOURCE']].drop_duplicates().values.tolist())

    # we get all parameters needed
    param_dict = get_inited_parameters(sr_snowflake_account,sr_gameday_output_init)
    logging.info(f"OUTPUT -> PARAM RETRIEVED")

    # we create messages from parameters for each country
    message_args= [(param_dict,context_dict['str_output_gameday_init_template_'+country],country,forum,sr_gameday_output_init) for (country,forum) in list_countries_forums]
    results = config.multithreading_run(create_inited_message, message_args)
    for content,country,forum in results:
        param_dict['MESSAGE_'+country+'_'+forum] = content
    logging.info(f"OUTPUT -> MESSAGES CREATED")

    # we post messages for each concerned topics
    posting_args = [(row,param_dict['MESSAGE_'+row['FORUM_COUNTRY']+'_'+row['FORUM_SOURCE']]) for _,row in df_topics.iterrows()]
    config.multithreading_run(post_message, posting_args)
    logging.info(f"OUTPUT -> GENERATING INITED MESSAGE [DONE]")