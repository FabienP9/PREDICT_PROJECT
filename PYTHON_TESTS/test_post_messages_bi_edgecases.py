'''
This tests file concern all functions in the get_messages_details_bi.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from message_actions.message_actions_bi import post_messages_bi as post_messages_bi
from testutils import assertExit

def test_post_message_bi_login_fail():
    
    # this test the function post_message_bi with login fails. Must exit program
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    message_content = "Test message"
    forum_source = topic_row['FORUM_SOURCE']

    # Mock session instance and os variables
    with patch.dict('os.environ', {
        f"{forum_source}_URL": "https://fakeforum.com",
        f"{forum_source}_USERNAME": "fake_user",
        f"{forum_source}_PASSWORD": "fake_pass"
    }),patch.object(post_messages_bi.requests,"Session") as mock_session, \
    patch.object(post_messages_bi.config, "time_message_wait", 1):
        
        mock_sess_instance = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_sess_instance

        # Mock POST responses failed
        mock_sess_instance.get.return_value.text = "Login failed"
        mock_sess_instance.post.return_value.text = "Login failed"

        assertExit(lambda: post_messages_bi.post_message_bi(topic_row, message_content,is_to_edit = 0))

def test_post_message_bi_times_out():
    
    # this test the function edit_message_bi with faiure to post. Must exit program after 3 tries    
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    fake_login_html = """
        <input name="sid" value="1"/>
        <input name="form_token" value="2"/>
        <input name="creation_time" value="3"/>
    """

    mock_session = MagicMock()
    mock_session.get.return_value.text = fake_login_html
    mock_session.post.return_value.text = "login failed"

    session_ctx = MagicMock()
    session_ctx.__enter__.return_value = mock_session
    session_ctx.__exit__.return_value = None

    with patch("requests.Session", return_value=session_ctx), \
         patch("os.getenv", return_value="http://fake-forum"), \
    patch.object(post_messages_bi.config, "time_message_wait", 1):

        assertExit(lambda: post_messages_bi.post_message_bi(topic_row, "timeout test",is_to_edit = 1))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_post_message_bi_login_fail))
    test_suite.addTest(unittest.FunctionTestCase(test_post_message_bi_times_out))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
