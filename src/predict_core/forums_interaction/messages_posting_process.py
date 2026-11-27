''' 
    The purpose of this module is to interact with forums website by:
    - getting the scope of topics: messages we want to extract
    - then extract the messages, and their details
    - and / or post messages calculated by the program
'''

import logging
import warnings
import pandas as pd

from ..config import config_decorators
from .forums_interaction_bi.messages_posting_process_bi import login_and_post_message_bi
from .forums_interaction_sh.messages_posting_process_sh import login_and_post_message_sh

logging.basicConfig(level=logging.INFO)

warnings.filterwarnings("ignore")
messages_post_functions = {
    "BI": login_and_post_message_bi,
    "SH": login_and_post_message_sh
}

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row',) })       
def post_edit_message(topic_row: pd.Series, message_main: str, message_small: str):

    '''
        Calls the function to run according to the forum source
        in order to post or edit a message 
        Inputs:
            topic_row (series - one row) to get the forum source
            message_main (str) the main message to post
            message_small (str) the small message to post (informing the user of an update of the main post with message_main)
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    
    post_function = messages_post_functions.get(topic_row.FORUM_SOURCE)
    # it depends on the kind of topic
    if topic_row.IS_FOR_PREDICT == 1:
        is_result = 0
    if topic_row.IS_FOR_RESULT == 1:
        is_result = 1 

    # if there is a post we want to edit
    if pd.notna(topic_row.MESSAGE_ID_TO_EDIT):
            
        # we edit the edit post with main message with header
        is_to_edit = 1
        post_function(topic_row,message_main,is_to_edit,is_result)

        # if message small is empty we post the main message to a new post 
        if message_small == "":
            is_to_edit = 0
            post_function(topic_row,message_main,is_to_edit,is_result)
        # else we post the small message
        else:
            is_to_edit = 0
            post_function(topic_row,message_small,is_to_edit,is_result)

    # if there is no message to edit on forum we just post the main message on a new post
    else:
        is_to_edit = 0
        post_function(topic_row,message_main,is_to_edit,is_result)