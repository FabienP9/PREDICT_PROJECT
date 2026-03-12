''' 
    The purpose of this module is to interact with forums website by:
    - getting the scope of topics: messages we want to extract
    - then extract the messages, and their details
    - and / or post messages calculated by the program
'''

import logging
import warnings
import os
import ssl
import urllib.request as urllib
from typing import Tuple

import pandas as pd
import pytz

from ..config import config_decorators
from ..config.config_variables import config_global_variables as var
from ..config.config_multithread import multithread_run
from ..files_manipulation.local_files_manipulation.files_manipulation import create_csv
from ..database_interaction.snowflake_connection_execution import snowflake_execute
from ..database_interaction.snowflake_etl_process import sql_queries as sql
from .forums_interaction_bi.messages_details_extraction_bi import get_messages_details_bi

logging.basicConfig(level=logging.INFO)
warnings.filterwarnings("ignore")
messages_info_functions = {
    "BI": get_messages_details_bi
}

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row',)})
@config_decorators.retry_function(log_filter=lambda args: {k: args[k] for k in ('topic_row',)})
def extract_messages_from_topic(topic_row: Tuple,ts_message_extract_min_utc: pd.Timestamp,ts_message_extract_max_utc: pd.Timestamp) -> pd.DataFrame | None:
        
    """
        Gets all messages in the time range of a topic
        Args:
            topic_row (tuple) : Contains basic info about the topic
            ts_message_extract_min_utc (timestamp utc): the min of the range of time for messages extraction
            ts_message_extract_max_utc (timestamp utc): the max of the range of time for messages extraction
        Returns:
            dataframe: contains all message in the time range from this topic, or None if there are no topics
        Raises:
            Retry 3 times and exits the program if error with url extraction (using retry decorator)
    """

    #we get min and max time range in the local time of the topic
    local_timezone = pytz.timezone(topic_row.FORUM_TIMEZONE)
    ts_message_extract_min_local = pytz.UTC.localize(ts_message_extract_min_utc).astimezone(local_timezone).strftime("%Y-%m-%d %H:%M:%S")
    ts_message_extract_max_local = pytz.UTC.localize(ts_message_extract_max_utc).astimezone(local_timezone).strftime("%Y-%m-%d %H:%M:%S")
    
    #Initial parameters
    forum_url = os.getenv(topic_row.FORUM_SOURCE + '_URL')
    start = 0
    seen_message_ids = set()
    topic_messages = []
    # we overwrite security for extracting from the website
    ssl_context = ssl._create_unverified_context() # NOSONAR # Disabled SSL verification for legacy forum with invalid certification
    
    while True:
        #we extract from the website page
        page_url = f"{forum_url}/viewtopic.php?t={topic_row.TOPIC_NUMBER}&start={start}"
        logging.info(f"MESSAGE -> EXTRACTING FORUM {topic_row.FORUM_SOURCE} / TOPIC {topic_row.TOPIC_NUMBER} / MESSAGES {start+1} -> X [START] ")
        response = urllib.urlopen(page_url, context=ssl_context)
        messagetext = response.read()
        messagetext = messagetext.decode('utf-8') 
        #we get all messages from the page
        get_messages_infos = messages_info_functions.get(topic_row.FORUM_SOURCE)
        df = get_messages_infos(messagetext, topic_row, start)
        current_ids = set(df['MESSAGE_FORUM_ID'])
        #if we already saw messages we stop here
        if current_ids.intersection(seen_message_ids):
            logging.info(f"MESSAGE -> BREAKING TOPIC {topic_row.TOPIC_NUMBER}")
            break
        #else we add those new message of the list of seen messages
        seen_message_ids.update(current_ids)
        start += len(df)

        # Filter on timestamp range (either creation date with none edition or edition date)
        creation_with_edition_none = df['CREATION_TIME_LOCAL'].between(ts_message_extract_min_local, ts_message_extract_max_local) & df['EDITION_TIME_LOCAL'].isna()
        edition = df['EDITION_TIME_LOCAL'].between(ts_message_extract_min_local, ts_message_extract_max_local)
        df_filtered = df[creation_with_edition_none | edition]

        if not df_filtered.empty:
            topic_messages.append(df_filtered)
        
        logging.info(f"MESSAGE -> EXTRACTING FORUM {topic_row.FORUM_SOURCE} / TOPIC {topic_row.TOPIC_NUMBER} / MESSAGES X -> {start} [DONE] ")
    return pd.concat(topic_messages, ignore_index=True) if topic_messages else None

@config_decorators.exit_program(log_filter=lambda args: {})
def extract_messages(sr_snowflake_account: pd.Series, sr_output_need: pd.Series) -> Tuple[pd.DataFrame, pd.Timestamp]:

    """
        Gets all messages we need to extract from the list of topics and time range
        Args:
            sr_snowflake_account (series - one row) : Contains snowflake parameters to run the query
            sr_output_need (series - one row): the output need file to get the range minimum of extraction
        Returns:
            - dataframe: contains all messages needed
            - datetime of extraction of messages
        Raises:
            Exits the program if error running the function (using decorator)
    """

    logging.info("MESSAGE -> EXTRACTING MESSAGES [START]")

    # we get the scope of topics and datetime range we need to extract messages
    topics_scope_id = get_list_topics_from_need(sr_snowflake_account, sr_output_need) 
    ts_message_extract_min_utc,ts_message_extract_max_utc = get_extraction_time_range(sr_snowflake_account, sr_output_need) 
    
    df_messages = pd.DataFrame(columns=['FORUM_SOURCE','TOPIC_NUMBER','USER',
                                            'MESSAGE_FORUM_ID','CREATION_TIME_LOCAL',
                                            'EDITION_TIME_LOCAL','MESSAGE_CONTENT'])

    # if the min >= max then we don't need to extract messages, there won't be any
    if ts_message_extract_min_utc >= ts_message_extract_max_utc:
        logging.info("MESSAGE -> no need to extract messages")
    else:
        # We parallelize the extraction of each topic    
        messages_args = [(row,ts_message_extract_min_utc,ts_message_extract_max_utc) 
                    for row in topics_scope_id.itertuples(index=False)]
        results = multithread_run(extract_messages_from_topic, messages_args)
        messages_extracted = [r for r in results if r is not None]

        if len(messages_extracted) > 0:
            df_messages = pd.concat(messages_extracted, ignore_index=True)

    create_csv(os.path.join(var.TMPF, 'message_check.csv'), df_messages, var.MESSAGE_FILE_ENCAPSULATED)
    logging.info("MESSAGE -> EXTRACTING MESSAGE [DONE]")
    return df_messages, ts_message_extract_max_utc

@config_decorators.exit_program(log_filter=lambda args: {})
def get_list_topics_from_need(sr_snowflake_account: pd.Series,sr_output_need: pd.Series) -> pd.DataFrame:

    """
        Gets list of topics where we extract messages
        Args:
            sr_snowflake_account (series - one row) : Contains snowflake paramaters to run the query
            sr_output_need (series - one row): the output_need we process - we extract only topics from its season
        Returns:
            dataframe: contains all topics number 
        Raises:
            Exits the program if error running the function (using decorator)
    """

    logging.info("MESSAGE -> GETTING TOPICS LIST [START]")

    topics_scope_list = snowflake_execute(sr_snowflake_account,sql.TOPICS_QUERY,sql.DATABASE,(sr_output_need['SEASON_ID'],))

    logging.info(f"Number of topics to extract: {len(topics_scope_list)}")
    logging.info("MESSAGE -> GETTING TOPICS LIST [DONE]")
    return topics_scope_list

@config_decorators.exit_program(log_filter=lambda args: {})
def get_extraction_time_range(sr_snowflake_account: pd.Series, sr_output_need: pd.Series) -> Tuple[pd.Timestamp, pd.Timestamp]:

    """
        Gets the time range for extraction of messages
        - min using check date detail from sr_output_need
        - max using the end time of the gameday on gameday snowflake table
        Args:
            sr_snowflake_account (series - one row) : Contains snowflake paramaters to run the query
            sr_output_need (series - one row): the output need file to get the range minimum of extraction
        Returns:
            min & max datetime of extraction in utc
        Raises:
            Exits the program if error running the function (using decorator)
    """

    # we extract the min with check time from need, we will limit extraction using it for range utc
    ts_message_extract_min_utc = pd.to_datetime(sr_output_need['LAST_MESSAGE_CHECK_TS_UTC'], errors='coerce')
    
    # we extract the max with Snowflake
    df_time_max = snowflake_execute(sr_snowflake_account,sql.GAMEDAY_END_DETAILS_QUERY, sql.DATABASE,(sr_output_need['SEASON_ID'],sr_output_need['GAMEDAY']))
    ts_message_extract_max_utc = pd.to_datetime(f"{df_time_max.at[0, 'END_DATE_UTC']} {df_time_max.at[0, 'END_TIME_UTC']}")
    
    #we get the current time utc
    current_time_utc = pd.Timestamp.utcnow().replace(microsecond=0).tz_localize(None)

    # if current_time_utc is before ts_message_extract_max_utc we replace ts_message_extract_max_utc
    ts_message_extract_max_utc = min(ts_message_extract_max_utc,current_time_utc)
    logging.info(f"MESSAGE -> Extracting between utc time {ts_message_extract_min_utc} and {ts_message_extract_max_utc} - extracting at {current_time_utc}")

    return ts_message_extract_min_utc,ts_message_extract_max_utc