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

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row',)})
def post_edit_message_bi(topic_row: pd.Series, message_content: str):

    '''
        Call Post and edit function on the "BI" forum conditionnally
        Inputs:
            topic_row (series - one row) details about the topic to know where to post, and if to edit
            message_content (str): message to post
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    if pd.notna(topic_row['MESSAGE_NUMBER_TO_EDIT']):
        
        date_update = datetime.now(ZoneInfo(topic_row['FORUM_TIMEZONE'])).strftime("%d/%m")
        if topic_row['IS_FOR_PREDICT'] == 1:
            string_update = "[i][Message auto-généré présentant la dernière ouverture de journée pour pronostics - Mis a jour le " + date_update + "][/i]"
            message_content_updated = message_content.replace(var.MESSAGE_PREFIX_PROGRAM_STRING,
                                                              var.MESSAGE_PREFIX_PROGRAM_STRING + "\n" + string_update + "\n")
            
            login_and_post_message_bi(topic_row, message_content_updated, is_to_edit = 1)
        
        if topic_row['IS_FOR_RESULT'] == 1:
            string_update = "[i][Message auto-généré présentant les derniers résultats - Mis a jour le " + date_update + "][/i]"
            message_content_updated = message_content.replace(var.MESSAGE_PREFIX_PROGRAM_STRING,
                                                              var.MESSAGE_PREFIX_PROGRAM_STRING+ "\n" + string_update + "\n")
            login_and_post_message_bi(topic_row, message_content_updated, is_to_edit = 1)

    login_and_post_message_bi(topic_row, message_content, is_to_edit = 0)

@config_decorators.exit_program(log_filter=lambda args: {})
def login_to_bi(session: requests.Session, login_url: str) -> Tuple[Literal[0, 1], str]:

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

    login_payload = {
        "username": os.getenv('BI_USERNAME'),
        "password": os.getenv('BI_PASSWORD'),
        "autologin": "on", 
        "login": "Connexion" 
    }
    login_get = session.get(login_url, verify=False) # NOSONAR
    login_get_successful = ('form_token' in login_get.text)
    if not login_get_successful:
        return False, ""
  
    try:
        soup = bs(login_get.text, "html.parser")
        login_payload['sid'] = soup.find("input", {"name": "sid"})["value"]
        login_payload['form_token'] = soup.find("input", {"name": "form_token"})["value"]
        login_payload['creation_time'] = soup.find("input", {"name": "creation_time"})["value"]
    except (KeyError, TypeError, AttributeError):
        return False, ""
    
    login_post = session.post(login_url, data=login_payload, verify=False) # NOSONAR
    login_post_successful = ('Déconnexion' in login_post.text)
    
    return login_post_successful, login_payload['sid']

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('is_to_edit',) })
def post_to_bi(message_content: str, session: requests.Session, post_url: str, sid: str, is_to_edit: Literal[0,1]):

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

    post_payload = {
        'sid' : sid,
        'message': message_content,
        'post': 'Envoyer'
    }
    
    post_get = session.get(post_url, verify=False) # NOSONAR
    post_get_successful = ('Déconnexion' in post_get.text and 'form_token' in post_get.text)

    if not post_get_successful:
        return False

    html_parser = "html.parser"
    soup = bs(post_get.text, html_parser)
    try:
        post_payload.update({
            "subject": soup.find("input", {"name": "subject"})["value"],
            "form_token": soup.find("input", {"name": "form_token"})["value"],
            "creation_time": soup.find("input", {"name": "creation_time"})["value"]
        })
        if is_to_edit == 1:
            post_payload.update({
                "edit_post_message_checksum": soup.find("input", {"name": "edit_post_message_checksum"})["value"],
                "edit_post_subject_checksum": soup.find("input", {"name": "edit_post_subject_checksum"})["value"],
                "show_panel": soup.find("input", {"name": "show_panel"})["value"]
            })
        else:
            post_payload.update({
                "topic_cur_post_id": soup.find("input", {"name": "topic_cur_post_id"})["value"]
            })
    except (KeyError, TypeError, AttributeError) :
        return False
    
    post_post  = session.post(post_url, data=post_payload, verify=False) # NOSONAR
    soup = bs(post_post.text, html_parser)
    post_post_successful = (soup.find("p", class_="error") is None)

    return post_post_successful    

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row','is_to_edit')})
@config_decorators.retry_function(log_filter=lambda args: {k: args[k] for k in ('topic_row','is_to_edit')})
def login_and_post_message_bi(topic_row: pd.Series, message_content: str, is_to_edit: Literal[0, 1]):

    '''
        Posts or edits a message on the "BI" forum
        Inputs:
            topic_row (series - one row) details about the topic to know where to post
            message_content (str): message to post
            is_to_edit (0/1): if 1 it will go through the edit script, else the post one
        Raises:
            If it didn't work to post/edit, we'll retry 3 times then exit program
    '''
    
    forum_url = os.getenv('BI_URL')
    time_max = var.TIME_MESSAGE_WAIT
    if is_to_edit == 1:
        logging.info(f"MESSAGES -> EDITING WITH NEW OUTPUT FOR BI / TOPIC {topic_row['TOPIC_NUMBER']} / MESSAGE {topic_row['MESSAGE_NUMBER_TO_EDIT']} - Trying for {time_max} secs [START] ")
        post_url = forum_url +"/posting.php?mode=edit&p="+ str(topic_row['MESSAGE_NUMBER_TO_EDIT'])
    else:
        logging.info(f"MESSAGES -> POSTING OUTPUT ON BI / TOPIC {topic_row['TOPIC_NUMBER']} - Trying for {time_max} secs [START] ")
        post_url = forum_url +"/posting.php?mode=reply&t="+ str(topic_row['TOPIC_NUMBER'])
    
    login_url = forum_url +'/ucp.php?mode=login'

    post_successful = False

    #We will try continuously to post for a specific period
    begin_time = time.time()
    time_at_expiration = begin_time + time_max
    while ((not post_successful) and (time.time() < time_at_expiration)):
        with requests.Session() as session:
            #We try to login
            login_successful, sid = login_to_bi(session,login_url)
            
            if login_successful:
                # we try to post
                post_successful = post_to_bi(message_content, session, post_url, sid, is_to_edit)
            
            if not (login_successful and post_successful):
                #if it didn't work we wait one second and retry
                time.sleep(1)
            else:
                if is_to_edit == 1:
                    logging.info(f"MESSAGES -> EDITING WITH NEW OUTPUT ON BI / TOPIC {topic_row['TOPIC_NUMBER']} / MESSAGE {topic_row['MESSAGE_NUMBER_TO_EDIT']} [DONE] ")
                else:
                    logging.info(f"MESSAGES -> POSTING OUTPUT ON BI / TOPIC {topic_row['TOPIC_NUMBER']} [DONE] ")

    if (not post_successful):
        raise ValueError(f"BI / TOPIC {topic_row['TOPIC_NUMBER']} NOT POSTED -> Time expiration")
