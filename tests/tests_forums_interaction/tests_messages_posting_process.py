'''
This tests file concern all functions in the messages_posting_process module.
It units test the happy path for each function
'''

from unittest.mock import MagicMock, patch

from src.predict_core.forums_interaction import messages_posting_process

def test_post_message(read_csv):

    topic_row = read_csv("q_topics_query.csv").iloc[0]
    message_content = "Hello, forum!"

    with patch.dict(messages_posting_process.messages_post_functions, {'BI': MagicMock()}):
        messages_posting_process.post_message(topic_row, message_content)
