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
import requests
import time
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup

from ...config import config_decorators
from ...config.config_variables import config_global_variables as var

def clean_content(html):
    soup = BeautifulSoup(html, "html.parser")
    # remove quotes nested inside quotes
    for inner in soup.select("blockquote blockquote"):
        inner.decompose()
    for bq in soup.find_all("blockquote"):
        bq.insert(0, "___BLOCKQUOTE_START___")               # marker as first child
        bq.append("___BLOCKQUOTE_END___")
    # extract plain text, then flatten newlines to keep one line per message
    text = soup.get_text(separator="\n", strip=True)
    return text.replace("\n", ' ;;;;; ')

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def get_username(topic_row: Tuple, headers: dict, user_id: pd.Series) -> pd.Series:

    unique_user_id: list[str] = user_id.dropna().unique().tolist()
    forum_url = os.getenv(topic_row.FORUM_SOURCE + '_URL')
    response = requests.get(forum_url+"/rest/v1/profiles", headers=headers,
                         params={"select": "id,username","id": f"in.({','.join(unique_user_id)})"}, timeout=30)
    if response.status_code != 200:
            raise ValueError(f"{var.SOFT_ERROR_TAG} Response {response.status_code} - Error {response.text[:500]}")

    rows: list[dict] = []
    rows.extend(response.json())
    lookup: dict[str, str] = {row["id"]: row["username"] for row in rows}
    missing: list[str] = [uid for uid in unique_user_id if uid not in lookup]
    if missing:
        raise ValueError(f"USER ID(s) without corresponding name: {missing}")

    return user_id.map(lookup).rename("username")

@config_decorators.raise_issue_to_caller(log_filter=lambda args: {})
def get_messages_details_sh(topic_row: Tuple,ts_message_extract_min_utc: pd.Timestamp,ts_message_extract_max_utc: pd.Timestamp) -> pd.DataFrame:

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
    df_messages = pd.DataFrame(columns=['user_id', 'id', 'created_at',
                                    'updated_at', 'content'])

    HEADERS = {
        "apikey": os.getenv('SH_API_KEY'),
        "authorization": f"Bearer {os.getenv('SH_API_KEY')}",
        "origin": forum_url.replace("api","www"),
        "prefer": "count=exact",
    }

    PARAMS = {
        "select": "user_id,id,created_at,updated_at,content",
        "topic_id": f"eq.{topic_row.TOPIC_ID}"
    }
    offset = 0
    total = None
    while True:
        logging.info(f"MESSAGE -> EXTRACTING FORUM {topic_row.FORUM_SOURCE} / TOPIC {topic_row.TOPIC_ID} / MESSAGES {offset+1} -> X [START] ")
        
        response = requests.get(forum_url+"/rest/v1/posts", headers=HEADERS,
                         params={**PARAMS, "offset": str(offset)}, timeout=30)
        if response.status_code != 200:
            raise ValueError(f"{var.SOFT_ERROR_TAG} Response {response.status_code} - Error {response.text[:500]}")

        # First response tells us the total, e.g. "0-99/4513"
        if total is None:
            content_range = response.headers.get("Content-Range", "")
            if "/" in content_range:
                total = int(content_range.split("/")[1])
        
        posts = response.json()
        if not posts:
            break
        df_messages_on_set = pd.DataFrame(posts)

        df_messages = pd.concat([df_messages, df_messages_on_set], ignore_index=True)
        offset += len(posts)

        if total is not None and offset >= total:
            logging.info(f"MESSAGE -> BREAKING FORUM {topic_row.FORUM_SOURCE} / TOPIC {topic_row.TOPIC_ID}")
            break
        time.sleep(0.1)
        logging.info(f"MESSAGE -> EXTRACTING FORUM {topic_row.FORUM_SOURCE} / TOPIC {topic_row.TOPIC_ID} / MESSAGES X -> {offset} [DONE] ")
    
    

    df_messages.insert(0, "FORUM_SOURCE", topic_row.FORUM_SOURCE)
    df_messages.insert(1, "TOPIC_ID", topic_row.TOPIC_ID)
    df_messages = df_messages.rename(columns={
        "id": "MESSAGE_FORUM_ID",
        "created_at": "CREATION_TIME_LOCAL",
        "updated_at": "EDITION_TIME_LOCAL",
        "content": "MESSAGE_CONTENT"
    })
    df_messages["CREATION_TIME_LOCAL"] = pd.to_datetime(df_messages["CREATION_TIME_LOCAL"]).dt.tz_convert(ZoneInfo(topic_row.FORUM_TIMEZONE)).dt.tz_localize(None) 
    df_messages["EDITION_TIME_LOCAL"] = pd.to_datetime(df_messages["EDITION_TIME_LOCAL"]).dt.tz_convert(ZoneInfo(topic_row.FORUM_TIMEZONE)).dt.tz_localize(None) 
    df_messages["MESSAGE_CONTENT"] = df_messages["MESSAGE_CONTENT"].apply(clean_content)
    df_messages.insert(2,"USER",get_username(topic_row, HEADERS, df_messages["user_id"]))
    df_messages = df_messages.drop(columns="user_id")
    
    creation_with_edition_none = df_messages['CREATION_TIME_LOCAL'].between(ts_message_extract_min_local, ts_message_extract_max_local) & df_messages['EDITION_TIME_LOCAL'].isna()
    edition = df_messages['EDITION_TIME_LOCAL'].between(ts_message_extract_min_local, ts_message_extract_max_local)
    df_messages = df_messages[creation_with_edition_none | edition]
        
    return df_messages
        