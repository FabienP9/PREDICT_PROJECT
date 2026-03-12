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
from .forums_interaction_bi.messages_posting_process_bi import post_edit_message_bi

logging.basicConfig(level=logging.INFO)

warnings.filterwarnings("ignore")
messages_post_functions = {
    "BI": post_edit_message_bi
}

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row',) })       
def post_message(topic_row: pd.Series, message_content: str):

    '''
        Calls the function to run according to the forum source
        in order to post a message 
        Inputs:
            topic_row (series - one row) to get the forum source
            message_content (str) the message to post
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    post_function = messages_post_functions.get(topic_row['FORUM_SOURCE'])
    post_function(topic_row,message_content)