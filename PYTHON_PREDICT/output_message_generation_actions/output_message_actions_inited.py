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

from datetime import datetime, timezone, timedelta
import pandas as pd
from typing import Tuple

from snowflake_actions import snowflake_execute
import config
from message_actions.message_actions import post_message
import file_actions.file_actions as fileA
from . import output_message_actions as outputA
from . import output_message_actions_sql_queries as sqlQ

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
                                df_games_copy['TEAM_AWAY_NAME'] + " ==> +1 ")
    
    # we create the LIST_GAMES string by concatenating all games-strings on several lines
    LIST_GAMES = "\n".join(df_games_copy['STRING'])
    
    #we add the bonus line
    if len(df_games_copy) > 0:
        bonus_line = f"#{df_games_copy.iloc[0]['GAMEDAY_MESSAGE']}.BN# __L__Bonus game ID__L__ ==> {df_games_copy.iloc[0]['GAME_MESSAGE']} "
        LIST_GAMES += "\n"+ bonus_line
    
    return LIST_GAMES

@config.exit_program(log_filter=lambda args: dict(args))
def transform_databasetime_for_output(df_cols_time: pd.Series) -> pd.Series:

    '''
        Transform a time HH:MM:SS to HhM for output
        Inputs:
            the time in format HH:MM:SS
        Returns:
            the time in format HhM
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    if df_cols_time.empty:
        return df_cols_time
    
    parts = df_cols_time.astype(str).str.split(":", expand=True)
    h = parts[0].astype(int).astype(str)
    m_str = parts[1]
    m_int = parts[1].astype(int)

    result = h + "h"
    result = result.where(m_int == 0, result + m_str.astype(str))

    return result

@config.exit_program(log_filter=lambda args: dict(args))
def transform_databasedate_for_output(df_cols_date: pd.Series) -> pd.Series:

    '''
        Transform a date YYYY-MM-DD to DD/MM for output
        Inputs:
            the time in format YYYY-MM-DD
        Returns:
            the time in format DD/MM
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    if df_cols_date.empty:
        return df_cols_date
    
    parts = df_cols_date.astype(str).str.split("-", expand=True)
    m = parts[1]
    d = parts[2]
    result = d + "/" + m
    return result

@config.exit_program(log_filter=lambda args: dict(args))
def add_time_to_databasedatetime(df_col_date: pd.Series, df_col_time: pd.Series, minutes_to_add: int) -> Tuple[pd.Series,pd.Series]:

    '''
        Add a defined amount of minute to a database date and time
        Inputs:
            the date in format in format YYYY-MM-DD and time in format HH:MM.SS
        Returns:
            date and time modified
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    if df_col_date.empty:
        return df_col_date,df_col_time
    

    dt = pd.to_datetime(
        df_col_date.astype(str) + " " + df_col_time.astype(str),
        format="%Y-%m-%d %H:%M:%S",
        errors="raise"
    )

    dt = dt + pd.to_timedelta(minutes_to_add, unit="m")

    return dt.dt.strftime("%Y-%m-%d"), dt.dt.strftime("%H:%M:%S")

@config.exit_program(log_filter=lambda args: dict(args))
def transform_inited_games_to_calendar(df_games: pd.DataFrame,) -> Tuple[str, dict]:

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
    df_games_copy["IS_RESULT"] = 0
    # We get the last game per gameday and add 2h30, to have the prediction result time then we add result time in the df calendar
    resultframe = (df_games_copy.sort_values(["DATE_GAME_LOCAL","TIME_GAME_LOCAL"]).groupby("GAMEDAY", as_index=False).last())
    
    resultframe["DATE_RESULT"], resultframe["TIME_RESULT"] = add_time_to_databasedatetime(resultframe["DATE_GAME_LOCAL"], resultframe["TIME_GAME_LOCAL"], minutes_to_add=150)
    resultrows = pd.DataFrame({
        "GAMEDAY": resultframe["GAMEDAY"],
        "GAME_MESSAGE": resultframe["GAMEDAY"],
        "DATE_GAME_LOCAL": resultframe["DATE_RESULT"],
        "TIME_GAME_LOCAL": resultframe["TIME_RESULT"],
        "IS_RESULT": 1
    })
    df_games_copy = pd.concat([df_games_copy, resultrows], ignore_index=True)
    # We transform date and time columns for output display
    df_games_copy['DATE_GAME_DISPLAY'] = transform_databasedate_for_output(df_games_copy['DATE_GAME_LOCAL'])
    df_games_copy['TIME_GAME_DISPLAY'] = transform_databasetime_for_output( df_games_copy['TIME_GAME_LOCAL'])
    
    # We create the multiline string of the calendar, group and sort by DATE_GAME_LOCAL and IS_RESULT
    df_games_copy = df_games_copy.sort_values(
        by=['DATE_GAME_LOCAL', 'IS_RESULT', 'TIME_GAME_LOCAL']
    )
    calendar_lines = []
    for (date_string, is_result), group in df_games_copy.groupby(['DATE_GAME_DISPLAY', 'IS_RESULT']):
        if is_result == 0:
            line = " / ".join(
                f"{row.GAME_MESSAGE} ({row.TIME_GAME_DISPLAY})"
                for _, row in group.iterrows()
            )
            calendar_lines.append(f"{date_string}: {line}")
        else:  # format for results
            for _, row in group.iterrows():
                calendar_lines.append(
                    f"{date_string} ~{row.TIME_GAME_DISPLAY}: "
                    f"__L__RESULTS OF PREDICTIONS OF__L__ {row.GAMEDAY}"
                )

    CALENDAR_GAMES = "\n".join(calendar_lines)

    # We finally extract the first game time per gameday
    firstgameframe = (df_games_copy.sort_values(["DATE_GAME_LOCAL","TIME_GAME_LOCAL"]).groupby("GAMEDAY", as_index=False).first())
    FIRSTGAMETIME_DICT = (
        firstgameframe.assign(todisplay=firstgameframe['DATE_GAME_DISPLAY'] + " " + firstgameframe['TIME_GAME_DISPLAY'])
                 .set_index('GAMEDAY')['todisplay']
                 .to_dict()
    )
    return CALENDAR_GAMES, FIRSTGAMETIME_DICT
    
@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_init',) })
def get_inited_next_opening_gamedays_calendar(sr_snowflake_account: pd.Series, sr_gameday_output_init: pd.Series) -> Tuple[str,int]:

    '''
        Gets the calendar of next opening gamedays, with their opening datetime, first game time, and results time
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials used in subfunction to run query
            sr_gameday_output_init (series - one row) containing parameters
        Returns:
            a multiline string with a one gameday per line
            The number of gamedays concerned
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # we get next gameday from tomorrow
    defined_date = (datetime.now(timezone.utc) + timedelta(days=1)).strftime('%Y-%m-%d')
    df_next_gamedays = snowflake_execute(sr_snowflake_account,sqlQ.qNextGamedaysOpening,(defined_date,sr_gameday_output_init['SEASON_ID']))    
    # we calculate the opening time 30 minutes after the init time - and the result time 2h30 after the last game time
    df_next_gamedays["DATE_TASK_30"], df_next_gamedays["TIME_TASK_30"] = add_time_to_databasedatetime(df_next_gamedays["DATE_TASK_LOCAL"], df_next_gamedays["TIME_TASK_LOCAL"], minutes_to_add=30)
    
    df_next_gamedays['DATE_TASK_30_DISPLAY'] = transform_databasedate_for_output(df_next_gamedays['DATE_TASK_30'])
    df_next_gamedays['TIME_TASK_30_DISPLAY'] = transform_databasetime_for_output( df_next_gamedays['TIME_TASK_30'])
    
    df_next_gamedays['BEGIN_DATE_DISPLAY'] = transform_databasedate_for_output(df_next_gamedays['BEGIN_DATE_LOCAL'])
    df_next_gamedays['BEGIN_TIME_DISPLAY'] = transform_databasetime_for_output( df_next_gamedays['BEGIN_TIME_LOCAL'])
    
    # we calculate the result time 2:30 minutes after the end game time 
    df_next_gamedays["END_DATE_150"], df_next_gamedays["END_TIME_150"] = add_time_to_databasedatetime(df_next_gamedays["END_DATE_LOCAL"], df_next_gamedays["END_TIME_LOCAL"], minutes_to_add=150)
    df_next_gamedays['END_DATE_150_DISPLAY'] = transform_databasedate_for_output(df_next_gamedays['END_DATE_150'])
    df_next_gamedays['END_TIME_150_DISPLAY'] = transform_databasetime_for_output( df_next_gamedays['END_TIME_150'])

    # we finally calculate the multiline string
    df_next_gamedays['STRING'] = df_next_gamedays['GAMEDAY'] + ": " + \
                                "__L__OPENING__L__ " + df_next_gamedays['DATE_TASK_30_DISPLAY'] + " ~" + df_next_gamedays['TIME_TASK_30_DISPLAY'] + " / " +\
                                 "__L__FIRST_GAME__L__ " + df_next_gamedays['BEGIN_DATE_DISPLAY'] + " "+ df_next_gamedays['BEGIN_TIME_DISPLAY'] + " / " +\
                                 "__L__PREDICTIONS_RESULTS__L__ " + df_next_gamedays['END_DATE_150_DISPLAY'] + " ~" + df_next_gamedays['END_TIME_150_DISPLAY']

    CALENDAR_NEXT_OPENING = "\n".join(df_next_gamedays['STRING'])
    return CALENDAR_NEXT_OPENING, len(df_next_gamedays)
    
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
    df_games_opened = snowflake_execute(sr_snowflake_account,sqlQ.qGame_Remaining_AtDate,(sr_gameday_output_init['SEASON_ID'],
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

    #parameters for next opening gamedays
    param_dict['CALENDAR_NEXT_OPENING'],param_dict['NB_NEXT_OPENING'] = get_inited_next_opening_gamedays_calendar(sr_snowflake_account, sr_gameday_output_init)

    param_dict['USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP'] = sr_gameday_output_init['USER_CAN_CHOOSE_TEAM_FOR_PREDICTCHAMP']
    return param_dict

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('country','forum','sr_gameday_output_init') })
def create_inited_message(param_dict: dict, template: str, translations_dict: dict, country: str, forum:str, sr_gameday_output_init: pd.Series) -> Tuple[str,str,str]:

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

    content = outputA.replace_conditionally_message(content, 
                                                "#NEXT_OPENING_BEGIN#", 
                                                "#NEXT_OPENING_END#", 
                                                param_dict['NB_NEXT_OPENING'] > 0) 
    
    if param_dict['NB_GAMES_OPENED'] > 0: 
        content = content.replace("#LIST_GAMEDAYS_OPENED#",param_dict['LIST_GAMEDAYS_OPENED'])
        content = content.replace("#LIST_GAMES_OPENED#",param_dict['LIST_GAMES_OPENED'])
        content = content.replace("#CALENDAR_GAMES_OPENED#",param_dict['CALENDAR_GAMES_OPENED'])

    if param_dict['NB_NEXT_OPENING'] > 0: 
        content = content.replace("#CALENDAR_NEXT_OPENING#",param_dict['CALENDAR_NEXT_OPENING'])

    #We then translate the content for the country and the forum
    content = outputA.translate_string(content,country,forum,translations_dict)

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
    message_args= [(param_dict,context_dict['str_output_gameday_init_template_'+country],context_dict['lst_output_gameday_template_translations'],country,forum,sr_gameday_output_init) for (country,forum) in list_countries_forums]
    results = config.multithreading_run(create_inited_message, message_args)
    for content,country,forum in results:
        param_dict['MESSAGE_'+country+'_'+forum] = content
    logging.info(f"OUTPUT -> MESSAGES CREATED")

    # we post messages for each concerned topics
    posting_args = [(row,param_dict['MESSAGE_'+row['FORUM_COUNTRY']+'_'+row['FORUM_SOURCE']]) for _,row in df_topics.iterrows()]
    config.multithreading_run(post_message, posting_args)
    logging.info(f"OUTPUT -> GENERATING INITED MESSAGE [DONE]")