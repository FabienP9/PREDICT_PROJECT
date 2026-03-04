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

def test_post_edit_message_bi():

    # this test the function post_edit_message_bi
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    message_content = "Hello BI forum!"

    with patch.object(post_messages_bi,"post_message_bi") as mock_post:

        post_messages_bi.post_edit_message_bi(topic_row, message_content)
        assert mock_post.call_count == 2

def test_post_message_bi_post():
    # this test the function post_message_bi for posting
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    message_content = "Test message"
    forum_source = topic_row['FORUM_SOURCE']

    # Mock session instance and os variables
    with patch.dict(
        "os.environ",
        {
            f"{forum_source}_URL": "https://fakeforum.com",
            f"{forum_source}_USERNAME": "fake_user",
            f"{forum_source}_PASSWORD": "fake_pass",
        },
    ), patch.object(post_messages_bi.requests,"Session") as mock_session:

        mock_sess_instance = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_sess_instance

        # --- Mock GET responses (login page, then reply page) ---
        login_html = """
        <form>
            <input name="sid" value="sid123">
            <input name="form_token" value="token123">
            <input name="creation_time" value="time123">
        </form>
        Déconnexion
        """

        reply_html = """
        <form>
            <input name="sid" value="sid123">
            <input name="form_token" value="token123">
            <input name="creation_time" value="time123">
            <input name="subject" value="Re: Test subject">
            <input name="topic_cur_post_id" value="topic123">
        </form>
        Déconnexion
        """

        mock_sess_instance.get.side_effect = [
            MagicMock(text=login_html),  # GET login page
            MagicMock(text=reply_html),  # GET reply page
        ]

        # --- Mock POST responses (login submit + message submit) ---
        mock_sess_instance.post.return_value.text = "Déconnexion"

        # Should not raise SystemExit
        post_messages_bi.post_message_bi(
            topic_row, message_content, is_to_edit=0
        )

def test_post_message_bi_edit():

    # this test the function post_message_bi for editing
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    message_content = "Test message"
    forum_source = topic_row["FORUM_SOURCE"]

    fake_login_html = """
        <html>
            <form>
                <input name="sid" value="sid123"/>
                <input name="form_token" value="token123"/>
                <input name="creation_time" value="time123"/>
            </form>
            Déconnexion
        </html>
    """

    fake_edit_page_html = """
        <html>
            <form>
                <input name="sid" value="sid123"/>
                <input name="subject" value="Edited subject"/>
                <input name="form_token" value="token123"/>
                <input name="creation_time" value="time123"/>
                <input name="edit_post_message_checksum" value="chk1"/>
                <input name="edit_post_subject_checksum" value="chk2"/>
                <input name="show_panel" value="1"/>
            </form>
            Déconnexion
        </html>
    """

    fake_post_success_html = "<html><body>Success</body></html>"

    mock_session = MagicMock()
    mock_session.get.side_effect = [
        MagicMock(text=fake_login_html),      # GET login
        MagicMock(text=fake_edit_page_html),  # GET edit page
    ]
    mock_session.post.side_effect = [
        MagicMock(text=fake_login_html),        # POST login
        MagicMock(text=fake_post_success_html)  # POST edited message
    ]

    mock_session_ctx = MagicMock()
    mock_session_ctx.__enter__.return_value = mock_session
    mock_session_ctx.__exit__.return_value = None

    with patch.dict(
        "os.environ",
        {
            f"{forum_source}_URL": "http://fake-forum",
            f"{forum_source}_USERNAME": "fake_user",
            f"{forum_source}_PASSWORD": "fake_pass",
        },
    ), patch.object(post_messages_bi.requests,"Session",return_value=mock_session_ctx):

        post_messages_bi.post_message_bi(topic_row, message_content, is_to_edit=1)

        # Basic behavioral assertions
        assert mock_session.get.call_count == 2
        assert mock_session.post.call_count == 2

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_post_edit_message_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_post_message_bi_post))
    test_suite.addTest(unittest.FunctionTestCase(test_post_message_bi_edit))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
