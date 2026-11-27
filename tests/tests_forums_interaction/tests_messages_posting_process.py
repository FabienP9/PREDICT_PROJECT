'''
This tests file concern all functions in the messages_posting_process module.
It units test the happy path for each function
'''

import pandas as pd

from src.predict_core.forums_interaction import messages_posting_process

def test_post_edit_message():

    #this test the post_edit_message function, editing with main message the posting small message
    calls = []

    def fake_post_function(topic_row, message, is_to_edit, is_result):
        calls.append((message, is_to_edit, is_result))

    original_dict = messages_posting_process.messages_post_functions
    messages_posting_process.messages_post_functions = {"forum_x": fake_post_function}

    try:
        topic_row = pd.Series({
            "FORUM_SOURCE": "forum_x",
            "IS_FOR_PREDICT": 1,
            "IS_FOR_RESULT": 0,
            "MESSAGE_NUMBER_TO_EDIT": 12345,  # not NaN -> "edit" branch
        })

        messages_posting_process.post_edit_message(topic_row, "main message", "small update message")

        assert calls == [
            ("main message", 1, 0),          # edit existing post with main message
            ("small update message", 0, 0),  # new post with the small message
        ]
    finally:
        messages_posting_process.messages_post_functions = original_dict
