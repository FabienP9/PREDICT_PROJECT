''' 
    The purpose of this module is to interact with "BI" forum by 
    posting and editing messages written by the program
'''

import logging
import os
import time
from datetime import datetime
from typing import Literal, Tuple
from zoneinfo import ZoneInfo
import pandas as pd
import requests
from bs4 import BeautifulSoup as bs

from ...config import config_decorators
from ...config.config_variables import config_global_variables as var

logging.basicConfig(level=logging.INFO)

@config_decorators.exit_program(log_filter=lambda args: {}, is_soft_error = 1)
def login_to_sh(topic_row: Tuple) -> Tuple[Literal[0, 1], str]:

    '''
        Attempt to login to the BI forum
        Inputs:
            session (requests.Session): the session to log in
            login_url (str): the url of the login page
        Returns:
            - a boolean (0/1) telling the sucess of the login
            - the sid (session id) of the logging
        Raises:
            Exit the porgram with issue with the function (using decorator)
    '''

    forum_url = os.getenv('SH_URL')
    response = requests.post(forum_url + "/auth/v1/token?grant_type=password",
                      headers={"apikey": os.getenv('SH_API_KEY'), 
                               "content-type": "application/json"},
                      json={"email": os.getenv('SH_USERNAME'), 
                            "password": os.getenv('SH_PASSWORD')},
                      timeout=30)
    if response.status_code != 200:
            raise ValueError(f"{var.SOFT_ERROR_TAG} Response {response.status_code} - Error {response.text[:500]}")

    data = response.json()
    token = data["access_token"]
    user_id = data["user"]["id"]
    return token, user_id

#@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('is_to_edit',) })
def post_to_sh(topic_row: Tuple, token: str, user_id: str, message_content: str):

    '''
        Attempt to post to the BI forum (while already logged in)
        Inputs:
            message_content (str): message to post
            session (requests.Session): the session while logged in
            post_url (str): the url of the post page
            sid (str): session id of the logging
            is_to_edit (0/1): if 1 it will go through the edit script, else the post one
        Returns:
            - a boolean (0/1) telling the sucess of the post
        Raises:
            Exit the porgram with issue with the function (using decorator)
    '''

    forum_url = os.getenv('SH_URL')
    response = requests.post(
        forum_url + "/rest/v1/posts?select=id",
        headers={
            "apikey": os.getenv('SH_API_KEY'),
            "authorization": f"Bearer {token}",
            "content-type": "application/json",
            "accept": "application/vnd.pgrst.object+json",
            "prefer": "return=representation",
            "origin": forum_url.replace("api","www"),
        },
        json={
            "content": message_content,
            "topic_id": topic_row.TOPIC_ID,
            "user_id": user_id,
        },
        timeout=30,
    )
    if response.status_code != 201:
        raise ValueError(f"{var.SOFT_ERROR_TAG} Response {response.status_code} - Error {response.text[:500]}")
  
def edit_to_sh(token, topic_row: Tuple, message_content: str):
    
    forum_url = os.getenv('SH_URL')
    response = requests.patch(
        forum_url + "/rest/v1/posts?select=id",
        params={"id": f"eq.{topic_row.MESSAGE_ID_TO_EDIT}"},
        headers={
            "apikey": os.getenv('SH_API_KEY'),
            "authorization": f"Bearer {token}",
            "content-type": "application/json",
        },
        json={"content": message_content},
        timeout=30,
    )
    if response.status_code != 204:
        raise ValueError(f"{var.SOFT_ERROR_TAG} Response {response.status_code} - Error {response.text[:500]}")


#@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row','is_to_edit')}, is_soft_error = 1)
#@config_decorators.retry_function(log_filter=lambda args: {k: args[k] for k in ('topic_row','is_to_edit')})
def login_and_post_message_sh(topic_row: pd.Series, message_content: str, is_to_edit: Literal[0, 1], is_result: Literal[0, 1]):

    '''
        Posts or edits a message on the "BI" forum
        Inputs:
            topic_row (series - one row) details about the topic to know where to post
            message_content (str): message to post
            is_to_edit (0/1): if 1 it will go through the edit script, else the post one
        Raises:
            If it didn't work to post/edit, we'll retry 3 times then exit program
    '''
    
    time_max = var.TIME_MESSAGE_WAIT
    token, user_id = login_to_sh(topic_row)
    if is_to_edit == 1:
        date_update = datetime.now(ZoneInfo(topic_row.FORUM_TIMEZONE)).strftime("%d/%m - %H:%M")
        if is_result == 1:
            string_update = "[i][Message auto-généré présentant la dernière ouverture de journée pour pronostics - Mis a jour le " + date_update + "][/i]"
        else:
            string_update = "[i][Message auto-généré présentant les derniers résultats - Mis a jour le " + date_update + "][/i]"
        
        message_content = message_content.replace(var.MESSAGE_PREFIX_PROGRAM_STRING,
                                                              var.MESSAGE_PREFIX_PROGRAM_STRING + "\n" + string_update + "\n")
        edit_to_sh(topic_row, token, user_id, message_content)
        logging.info(f"MESSAGES -> EDITING WITH NEW OUTPUT FOR SH / TOPIC {topic_row.TOPIC_ID} / MESSAGE {topic_row.MESSAGE_ID_TO_EDIT} - Trying for {time_max} secs [START] ")

    else:
        
        post_to_sh(topic_row, token, user_id, message_content)
        logging.info(f"MESSAGES -> POSTING OUTPUT ON SH / TOPIC {topic_row.TOPIC_ID} - Trying for {time_max} secs [START] ")
        
