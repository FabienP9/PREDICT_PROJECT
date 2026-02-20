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
import numpy as np
from datetime import datetime, date

from snowflake_actions import snowflake_execute
import config
from message_actions import post_message
import file_actions as fileA
from output_actions import output_actions as outputA
from output_actions import output_actions_sql_queries as sqlQ

@config.exit_program(log_filter=lambda args: dict(args))
def transform_inited_games_to_list(df_games: pd.DataFrame) -> str:

    '''
        Transform a dataframe of games of a single gameday to a list
        Inputs:
            df_games (dataframe) containing games to list
        Returns:
            A multiple row string displaying the list of games
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    df_games_copy = df_games.copy()
    df_games_copy['STRING'] = ("#" + df_games_copy['GAME_MESSAGE'] + "# " +
                                df_games_copy['TEAM_HOME_NAME'] + " vs " +
                                df_games_copy['TEAM_AWAY_NAME'] + " ==> __F__italicbegin__F__ +1 __F__italicend__F__")
    
    # we create the LIST_GAMES string by concatenating all games-strings on several lines
    LIST_GAMES = "\n".join(df_games_copy['STRING'])
    
    #we add the bonus line
    if len(df_games_copy) > 0:
        bonus_line = f"#{df_games_copy.iloc[0]['GAMEDAY_MESSAGE']}.BN# __L__Bonus game ID__L__ ==> __F__italicbegin__F__ {df_games_copy.iloc[0]['GAME_MESSAGE']} __F__italicend__F__"
        LIST_GAMES += "\n"+ bonus_line
    
    return LIST_GAMES

@config.exit_program(log_filter=lambda args: dict(args))
def transform_inited_games_to_calendar(df_games: pd.DataFrame) -> Tuple[str,dict]:

    '''
        Transform a dataframe of games of multiple gameday to a calendar, with game datetimes, and prediction result time
        Inputs:
            df_games (dataframe) containing games to calendar
        Returns:
            - A multiple row string displaying the calendar per day (str)
            - Each gameday with its first game datetime (dict)
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    df_games_copy = df_games.copy()[['GAMEDAY','GAME_MESSAGE','DATE_GAME_LOCAL','TIME_GAME_LOCAL']]
    df_games_copy["DATE_GAME_LOCAL"] = pd.to_datetime(df_games_copy["DATE_GAME_LOCAL"])
    df_games_copy["TIME_GAME_LOCAL"] = df_games_copy["TIME_GAME_LOCAL"].apply(lambda t: datetime.combine(date.today(), t))
    df_games_copy["DATETIME"] = pd.to_datetime(df_games_copy["DATE_GAME_LOCAL"].astype(str) + " " + df_games_copy["TIME_GAME_LOCAL"].astype(str)).dt.tz_localize(None)
    df_games_copy["IS_RESULT"] = 0
    
    lastgametime = (
        df_games_copy.sort_values("DATETIME")
        .groupby("GAMEDAY", as_index=False)
        .last()
    )
    lastgametime["DATETIME_RESULTS"] = lastgametime["DATETIME"].dt.tz_localize(None) + pd.Timedelta(hours=2, minutes=15)

    result_rows = pd.DataFrame({
        "GAMEDAY": lastgametime["GAMEDAY"],
        "GAME_MESSAGE": lastgametime["GAMEDAY"],
        "DATE_GAME_LOCAL": lastgametime["DATETIME_RESULTS"],
        "TIME_GAME_LOCAL": lastgametime["DATETIME_RESULTS"],
        "DATETIME" : lastgametime["DATETIME_RESULTS"],
        "IS_RESULT":1
    })

    df_games_copy = pd.concat([df_games_copy, result_rows],ignore_index=True)

    # We transform date and time columns for displaying them on forums
    df_games_copy['DATE_DDMM_STRING'] = df_games_copy['DATE_GAME_LOCAL'].dt.strftime('%d/%m')
    df_games_copy['TIME_HhM_STRING'] = df_games_copy["TIME_GAME_LOCAL"].dt.strftime("%H") + "h" +\
                                        np.where(df_games_copy["TIME_GAME_LOCAL"].dt.minute != 0,
                                                df_games_copy["TIME_GAME_LOCAL"].dt.strftime("%M"),
                                                "")
    df_games_copy['DATETIME_TO_DISPLAY'] = df_games_copy['DATE_DDMM_STRING'] + " " + df_games_copy['TIME_HhM_STRING']  
   
    df_games_copy = df_games_copy.sort_values(by=['DATE_GAME_LOCAL', 'IS_RESULT', 'TIME_GAME_LOCAL'])
    
    #We create the multiline string of the calendar, group by DATE_GAME_LOCAL and IS_RESULT
    calendar_lines = []
    for (date_string, is_result), group in df_games_copy.groupby(['DATE_DDMM_STRING', 'IS_RESULT']):
        if is_result == 0:
        
            line = " / ".join(f"{row.GAME_MESSAGE} ({row.TIME_HhM_STRING})" for idx, row in group.iterrows())
            calendar_lines.append(f"{date_string}: {line}")
    else: #format for results
        for _, row in group.iterrows():
            calendar_lines.append(f"{date_string} ~{row.TIME_HhM_STRING}: __L__PLANNED RESULTS OF YOUR PREDICTIONS OF GAMEDAY__L__ {row.GAMEDAY}")

    CALENDAR_GAMES = "\n".join(calendar_lines)

    #We finally extract the first game time per gameday
    FIRSTGAMETIME_DICT = (
        df_games_copy.loc[df_games_copy.groupby("GAMEDAY")["DATETIME"].idxmin()]
        .set_index("GAMEDAY")["DATETIME_TO_DISPLAY"]
        .to_dict())
    
    return CALENDAR_GAMES, FIRSTGAMETIME_DICT

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

    #parameters from the opening gameday
    param_dict['GAMEDAY_OPENING'] = sr_gameday_output_init['GAMEDAY']
    df_games_opening = snowflake_execute(sr_snowflake_account,sqlQ.qGame,(sr_gameday_output_init['SEASON_ID'],sr_gameday_output_init['GAMEDAY']))
    param_dict['NB_GAMES_OPENING'] = len(df_games_opening)
    if param_dict['NB_GAMES_OPENING'] > 0:
        param_dict['CALENDAR_GAMES_OPENING'], FIRSTGAMETIME_DICT_OPENING = transform_inited_games_to_calendar(df_games_opening)
        param_dict['FIRSTGAMETIME_OPENING'] = f"__L__{sr_gameday_output_init['BEGIN_DATE_WEEKDAY']}__L__ {FIRSTGAMETIME_DICT_OPENING[param_dict['GAMEDAY_OPENING']]}"
        param_dict['LIST_GAMES_OPENING'] = transform_inited_games_to_list(df_games_opening)
    
    #parameters from gamedays already opened (at defined date)
    defined_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    df_games_opened = snowflake_execute(sr_snowflake_account,sqlQ.qGame_Remaining_AtDate,(defined_date,
                                                                                 sr_gameday_output_init['SEASON_ID'],
                                                                                 sr_gameday_output_init['GAMEDAY'],
                                                                                 defined_date,defined_date,defined_date))
    param_dict['NB_GAMES_OPENED'] = len(df_games_opened)
    if param_dict['NB_GAMES_OPENED'] > 0:
        param_dict['LIST_GAMEDAYS_OPENED'] = " , ".join(df_games_opened['GAMEDAY'].unique())
        param_dict['CALENDAR_GAMES_OPENED'], _ = transform_inited_games_to_calendar(df_games_opened)
        
        df_gamedays_opened = df_games_opened.groupby("GAMEDAY")
        gameday_args = [(df_gameday_opened,) for _, df_gameday_opened in df_gamedays_opened]
        results_string = config.multithreading_run(transform_inited_games_to_list, gameday_args)
        param_dict['LIST_GAMES_OPENED'] = "\n".join(results_string)

    param_dict['USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP'] = sr_gameday_output_init['USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP']
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
    content = content.replace("#GAMEDAY_OPENING#",param_dict['GAMEDAY_OPENING'])  
    content = content.replace("#FIRSTGAMETIME_OPENING#",param_dict['FIRSTGAMETIME_OPENING'])  
    content = content.replace("#LIST_GAMES_OPENING#",param_dict['LIST_GAMES_OPENING'])  
    content = content.replace("#CALENDAR_GAMES_OPENING#",param_dict['CALENDAR_GAMES_OPENING'])

    # ... and on condition
    content = outputA.replace_conditionally_message(content, 
                                            "#USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP_BEGIN#", 
                                            "#USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP_END#", 
                                            param_dict['USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP'] == 1)
    
    content = outputA.replace_conditionally_message(content, 
                                                "#REMAINING_GAMES_BEGIN#", 
                                                "#REMAINING_GAMES_END#", 
                                                param_dict['NB_GAMES_OPENED'] > 0)   
    
    if param_dict['NB_GAMES_OPENED'] > 0: 
        content = content.replace("#LIST_GAMEDAYS_OPENED#",param_dict['LIST_GAMEDAYS_OPENED'])
        content = content.replace("#LIST_GAMES_OPENED#",param_dict['LIST_GAMES_OPENED'])
        content = content.replace("#CALENDAR_GAMES_OPENED#",param_dict['CALENDAR_GAMES_OPENED'])

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