'''
    The purpose of this module is to extract messages details coming from the BI forum 
''' 
import logging
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup as bs
from typing import Tuple
import os
import ssl
import socket
import urllib.request
import urllib.error
import pytz

from ...config import config_decorators
from ...config.config_variables import config_global_variables as var

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def translate_french_special_date_to_english(date_string: str) -> str:

    """
        Transforms a French forum special date type to english
        Args:
            date_string (string) : The string corresponding to the forum special french date
        Returns:
            the transformed string
        Raises:
            Raise the issue to the caller if exception
    """

    french_to_english = {
    "janv.": "Jan", "févr.": "Feb", "mars": "Mar", "avr.": "Apr", "mai": "May", "juin": "Jun",
    "juil.": "Jul", "août": "Aug", "sept.": "Sep", "oct.": "Oct", "nov.": "Nov", "déc.": "Dec",
    "lun.": "Mon",  "mar.": "Tue", "mer.": "Wed", "jeu.": "Thu", "ven.": "Fri", "sam.": "Sat", "dim.": "Sun"
    }
    for french, english in french_to_english.items():
        date_string = date_string.replace(french, english)
    return date_string

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def transform_forum_time_to_datetime(date_string: str) -> datetime:

    """
        Transforms a forum date type into a datetime
        Args:
            date_string (string) : The string corresponding to the forum date translated in english
        Returns:
            the datetime corresponding to the string
        Raises:
            Raise the issue to the caller if exception
    """
    
    input_format = "%a %d %b %Y %H:%M"
    # Parse the input date string
    parsed_date = datetime.strptime(date_string, input_format)
    return parsed_date

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def get_users_bi(soup: bs) -> list[str]:

    """
        Gets the list of users who posted from the forum HTML page
        Args:
            soup (BeautifulSoup) : The beautiful soup string to parse
        Returns:
            list of string which represent user names
        Raises:
            Raise the issue to the caller if exception
    """

    #We parse using BeautifulSoup
    users = [
        a_tag.text.strip()
        for span in soup.find_all('span', class_='responsive-hide')
        if (a_tag := span.find('a', class_='username'))
    ]
    return users

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def get_ids_bi(soup: bs) -> list[str]:

    """
        Gets the list of message ids: it is unique for each message
        Args:
            soup (BeautifulSoup) : The beautiful soup string to parse
        Returns:
            list of string id of messages
        Raises:
            Raise the issue to the caller if exception
    """    

    #We parse using BeautifulSoup
    ids = [
        int(id_tag.get('id').replace('profile', ''))
        for id_tag in soup.find_all('dl', class_='postprofile') 
        if id_tag.get('id') and isinstance(id_tag.get('id'), str)
    ]
    return ids

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def get_creationtimes_bi(soup: bs) -> list[datetime]:

    """
        Gets the list of messages creation time
        Args:
            soup (BeautifulSoup) : The beautiful soup string to parse
        Returns:
            list of timestamp representing the creation time
        Raises:
            Raise the issue to the caller if exception
    """    
    #We parse using BeautifulSoup and transform the french forum special date to timestamp
    #French forum date: Lun. 01 Fev 2025 9:08:00 -> Return date: 2025-02-01 09:08:00
    creationtimes = [
            transform_forum_time_to_datetime(
                translate_french_special_date_to_english(
                    time_tag.get_text(strip=True)
                )
            )
            for time_tag in soup.find_all("time")
        ]
    return creationtimes

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def get_contents_outerblockquote_bi(soup) -> list[str]:

    """
        Gets the content of all messages, with their outer blockquote if any
        Args:
            soup (BeautifulSoup) : The beautiful soup string to parse
        Returns:
            list of string with the content of messages shorten on one line, possibly with outer blockquotes
        Raises:
            Raise the issue to the caller if exception
    """  

    #we remove tags except outer blockquote ones
    def keep_only_outer_blockquote_tags(html):

        intern_soup = bs(html, 'html.parser')
        # Find all blockquotes nested inside other blockquotes and unwrap them
        for nested in intern_soup.select('blockquote blockquote'):
            nested.unwrap()  # Remove only the tag, keep content

        # Now replace the remaining blockquotes with placeholders
        for bq in intern_soup.find_all('blockquote'):
            bq.insert_before('___BLOCKQUOTE_START___')
            bq.insert_after('___BLOCKQUOTE_END___')
            bq.unwrap()

        text = intern_soup.get_text()

        # Restore blockquote tags only for the outer ones
        text = text.replace('___BLOCKQUOTE_START___', '<blockquote>')
        text = text.replace('___BLOCKQUOTE_END___', '</blockquote>')

        return text

    # Now apply the transformation
    contents = [
        keep_only_outer_blockquote_tags(str(div))
        for div in soup.find_all('div', class_='content')
    ]
    #For a better messages readibility on the database we replace all "new line" to have the message content in one line
    contents = [c.replace('\n', ' ;;;;; ') for c in contents]

    return contents

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def get_editiontimes_bi(soup: bs) -> list[datetime | None]:

    """
        Gets the edition time of all messages if they exist (messages edited)
        Args:
            soup (BeautifulSoup) : The beautiful soup string to parse
        Returns:
            list of None (if not edited) or timestamp corresponding to the edition time
        Raises:
            Raise the issue to the caller if exception
    """
    
    #We parse using BeautifulSoup
    # Find all messages
    messages = soup.find_all("div", class_="postbody")
    
    result = []

    # Iterate over all messages and extract the cleaned edition string
    for message in messages:
        # Search for the notice containing "Modifié en dernier par"
        notice = message.find("div", class_="notice")
        if notice:
            # Extract the edition string text
            text = notice.get_text(strip=True)
            # Find the part of the text starting with "Modifié en dernier par"
            if "Modifié en dernier par" in text:
                raw_date = text.split("Modifié en dernier par")[1].strip()
                # Clean the string to remove the username and extra details
                parts = raw_date.split("le ")
                if len(parts) > 1:
                    result.append(parts[1].split(",")[0].strip())
                else:
                    result.append(None)
            else:
                result.append(None)
        else:
            result.append(None)
            
    #We transform special type french forum dates into a timestamp
    #French forum date: Lun. 01 Fev 2025 9:08:00 -> Return date: 2025-02-01 09:08:00
    result_en = [translate_french_special_date_to_english(res) if res is not None else None for res in result]
    result_date = [transform_forum_time_to_datetime(res) if res is not None else None for res in result_en]
    
    return result_date

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def get_messages_details_bi(topic_row: Tuple,ts_message_extract_min_utc: pd.Timestamp,ts_message_extract_max_utc: pd.Timestamp) -> pd.DataFrame:

    """
        Gets details from all messages
        Args:
            messagetext (string) : The HTML list of messages
            topic_row (series): information about the topic
            start (int): for logging purpose
        Returns:
            the dataframe corresponding to the details extracted (one row per message)
        Raises:
            Raise the issue to the caller if exception
    """

    #we get min and max time range in the local time of the topic
    local_timezone = pytz.timezone(topic_row.FORUM_TIMEZONE)
    ts_message_extract_min_local = pytz.UTC.localize(ts_message_extract_min_utc).astimezone(local_timezone).strftime("%Y-%m-%d %H:%M:%S")
    ts_message_extract_max_local = pytz.UTC.localize(ts_message_extract_max_utc).astimezone(local_timezone).strftime("%Y-%m-%d %H:%M:%S")
    
    #Initial parameters
    forum_url = os.getenv(topic_row.FORUM_SOURCE + '_URL')
    start = 0
    seen_message_ids = set()
    # we overwrite security for extracting from the website
    ssl_context = ssl._create_unverified_context() # NOSONAR # Disabled SSL verification for legacy forum with invalid certification
    
    df_messages = pd.DataFrame(columns=['FORUM_SOURCE', 'TOPIC_ID', 'USER',
                                    'MESSAGE_FORUM_ID', 'CREATION_TIME_LOCAL',
                                    'EDITION_TIME_LOCAL', 'MESSAGE_CONTENT'])

    #we extract from the website page
    while True:
        page_url = f"{forum_url}/viewtopic.php?t={topic_row.TOPIC_ID}&start={start}"
        logging.info(f"MESSAGE -> EXTRACTING FORUM {topic_row.FORUM_SOURCE} / TOPIC {topic_row.TOPIC_ID} / MESSAGES {start+1} -> X [START] ")
        try:
            with urllib.request.urlopen(page_url, context=ssl_context, timeout=var.TIME_MESSAGE_WAIT) as response:
                messagetext = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                raise ValueError(f"{var.SOFT_ERROR_TAG} PAGE {page_url} -> server error "
                                f"(status {e.code}): {e.read()[:300]}") from e
            raise ValueError(f"PAGE {page_url} -> request rejected (status {e.code})") from e
        except urllib.error.URLError as e:
            if isinstance(e.reason, (socket.gaierror, socket.timeout, ConnectionRefusedError)):
                raise ValueError(f"{var.SOFT_ERROR_TAG} PAGE {page_url} -> network issue: {e.reason}") from e
            raise ValueError(f"PAGE {page_url} -> url error: {e.reason}") from e
        except (socket.timeout, TimeoutError) as e:
            raise ValueError(f"{var.SOFT_ERROR_TAG} PAGE {page_url} -> timed out: {e}") from e

        #we calculate for logging purpose
        log_print = f"{topic_row.FORUM_SOURCE} / {topic_row.TOPIC_ID} / {start}"

        #We get the details
        soup = bs(messagetext, "html.parser")
        users = get_users_bi(soup)
        ids = get_ids_bi(soup)
        creation_times_local = get_creationtimes_bi(soup)
        contents = get_contents_outerblockquote_bi(soup)
        edition_times_local = get_editiontimes_bi(soup)

        #if not all list are same size we raise an error to the exit_program decorator
        lengths = {
            'users': len(users),
            'ids': len(ids),
            'creation': len(creation_times_local),
            'contents': len(contents),
            'edition': len(edition_times_local),
        }
        if len(set(lengths.values())) != 1:
            raise ValueError(f"Mismatch extracting messages. {log_print} -> {lengths}")

        df_messages_on_page = pd.DataFrame({
            'FORUM_SOURCE': topic_row.FORUM_SOURCE,
            'TOPIC_ID': topic_row.TOPIC_ID,
            'USER': users,
            'MESSAGE_FORUM_ID': ids,
            'CREATION_TIME_LOCAL': creation_times_local,
            'EDITION_TIME_LOCAL': edition_times_local,
            'MESSAGE_CONTENT': contents
        }) 

        # Filter on timestamp range (either creation date with none edition or edition date)
        creation_with_edition_none = df_messages_on_page['CREATION_TIME_LOCAL'].between(ts_message_extract_min_local, ts_message_extract_max_local) & df_messages_on_page['EDITION_TIME_LOCAL'].isna()
        edition = df_messages_on_page['EDITION_TIME_LOCAL'].between(ts_message_extract_min_local, ts_message_extract_max_local)
        df_filtered = df_messages_on_page[creation_with_edition_none | edition]
        df_messages = pd.concat([df_messages, df_filtered], ignore_index=True)
        
        current_ids = set(df_messages_on_page['MESSAGE_FORUM_ID'])
        #if we already saw messages we stop here
        if current_ids.intersection(seen_message_ids):
            logging.info(f"MESSAGE -> BREAKING FORUM {topic_row.FORUM_SOURCE} / TOPIC {topic_row.TOPIC_ID}")
            break
        #else we add those new message of the list of seen messages
        seen_message_ids.update(current_ids)
        start += len(df_messages_on_page)
        logging.info(f"MESSAGE -> EXTRACTING FORUM {topic_row.FORUM_SOURCE} / TOPIC {topic_row.TOPIC_ID} / MESSAGES X -> {start} [DONE] ")
    
    df_messages = df_messages.drop_duplicates().reset_index(drop=True)
    return df_messages