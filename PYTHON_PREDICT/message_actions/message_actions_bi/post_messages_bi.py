''' 
    The purpose of this module is to interact with "BI" forum by 
    posting and editing messages written by the program
'''

import logging
logging.basicConfig(level=logging.INFO)
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Literal
import time
import os
import requests
from bs4 import BeautifulSoup as bs

import config

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row',)})
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
            message_content_updated = message_content.replace(config.message_prefix_program_string,
                                                              config.message_prefix_program_string + "\n" + string_update + "\n")
            
            post_message_bi(topic_row, message_content_updated, is_to_edit = 1)
        
        if topic_row['IS_FOR_RESULT'] == 1:
            string_update = "[i][Message auto-généré présentant les derniers résultats - Mis a jour le " + date_update + "][/i]"
            message_content_updated = message_content.replace(config.message_prefix_program_string,
                                                              config.message_prefix_program_string+ "\n" + string_update + "\n")
            post_message_bi(topic_row, message_content_updated, is_to_edit = 1)

    post_message_bi(topic_row, message_content, is_to_edit = 0)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row','is_to_edit')})
@config.retry_function(log_filter=lambda args: {k: args[k] for k in ('topic_row','is_to_edit')})
def post_message_bi(topic_row: pd.Series, message_content: str, is_to_edit: Literal[0, 1]):

    '''
        Posts or edits a message on the "BI" forum
        Inputs:
            topic_row (series - one row) details about the topic to know where to post
            message_content (str): message to post
            is_to_edit (0/1): if 1 it will go through the edit script, else the post one
        Raises:
            If it didn't work to post/edit, we'll retry 3 times then exit program
    '''

    #We will try to post for a specific time max
    time_max = config.time_message_wait    
    begin_time = time.time()
    
    forum_url = os.getenv(topic_row['FORUM_SOURCE'] + '_URL')
    if is_to_edit == 1:
        logging.info(f"MESSAGES -> EDITING WITH NEW OUTPUT FOR {topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} / MESSAGE {topic_row['MESSAGE_NUMBER_TO_EDIT']} - Trying for {time_max} secs [START] ")
        post_url = forum_url +"/posting.php?mode=edit&p="+ str(topic_row['MESSAGE_NUMBER_TO_EDIT'])
    else:
        logging.info(f"MESSAGES -> POSTING OUTPUT ON {topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} - Trying for {time_max} secs [START] ")
        post_url = forum_url +"/posting.php?mode=reply&t="+ str(topic_row['TOPIC_NUMBER'])
    
    login_url = forum_url +'/ucp.php?mode=login'

    connect_dict = {
        "username": os.getenv(topic_row['FORUM_SOURCE']+'_USERNAME'),
        "password": os.getenv(topic_row['FORUM_SOURCE']+'_PASSWORD'),
        "autologin": "on", 
        "login": "Connexion" 
    }

    messagepost_dict = {
        'message': message_content,
        'post': 'Envoyer'
    }

    message_post_worked = False

    while (message_post_worked == False and time.time() - begin_time <= time_max):
        with requests.Session() as session:
            #We get the login page
            connect_get = session.get(login_url, verify=False)
            connect_get_worked = ('form_token' in connect_get.text)
            if connect_get_worked:
                soup = bs(connect_get.text, "html.parser")
                connect_dict['sid'] = soup.find("input", {"name": "sid"})["value"]
                connect_dict['form_token'] = soup.find("input", {"name": "form_token"})["value"]
                connect_dict['creation_time'] = soup.find("input", {"name": "creation_time"})["value"]        

                # We post the dictionary to login
                connect_post = session.post(login_url, data=connect_dict, verify=False)
                connect_post_worked = ('Déconnexion' in connect_post.text)
                if connect_post_worked:
                    reply_get = session.get(post_url, verify=False)
                    reply_get_worked = ('Déconnexion' in reply_get.text and 'form_token' in reply_get.text)

                    if reply_get_worked:
                        # if it worked we post the dictionary to post the message
                        soup = bs(reply_get.text, "html.parser")
                        messagepost_dict.update({
                            "sid" : connect_dict['sid'],
                            "subject": soup.find("input", {"name": "subject"})["value"],
                            "form_token": soup.find("input", {"name": "form_token"})["value"],
                            "creation_time": soup.find("input", {"name": "creation_time"})["value"]
                        })
                        if is_to_edit == 1:
                            messagepost_dict.update({
                                "edit_post_message_checksum": soup.find("input", {"name": "edit_post_message_checksum"})["value"],
                                "edit_post_subject_checksum": soup.find("input", {"name": "edit_post_subject_checksum"})["value"],
                                "show_panel": soup.find("input", {"name": "show_panel"})["value"]
                            })
                        else:
                            messagepost_dict.update({
                                "topic_cur_post_id": soup.find("input", {"name": "topic_cur_post_id"})["value"]
                            })

                        message_post  = session.post(post_url, data=messagepost_dict, verify=False)
                        soup = bs(message_post.text, "html.parser")
                        message_post_worked = (soup.find("p", class_="error") == None)

        if message_post_worked == False:
            #otherwise we wait one second and retry
            time.sleep(1)

    if (message_post_worked == False):
        raise ValueError(f"{topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} NOT POSTED -> Time expiration")
    elif is_to_edit == 1:
        logging.info(f"MESSAGES -> EDITING WITH NEW OUTPUT ON {topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} / MESSAGE {topic_row['MESSAGE_NUMBER_TO_EDIT']} [DONE] ")
    else:
        logging.info(f"MESSAGES -> POSTING OUTPUT ON {topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} [DONE] ")
