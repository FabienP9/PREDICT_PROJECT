'''
This tests file concern all functions in the message_actions module.
It units test unhappy paths for each function
'''
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import message_actions
from testutils import assertExit

def test_extract_messages_from_topic_invalid_timezone():
    
    # this test the function extract_messages_from_topic with topic row contains invalid timezone. Must exit program
    topic_row = pd.read_csv("materials/edgecases/qTopics_Calculate_invalid_timezone.csv").iloc[0]
    ts_message_extract_min_utc = pd.Timestamp('2025-02-04 21:00:00')
    ts_message_extract_max_utc = pd.Timestamp('2025-02-04 22:30:00')

    mock_response = MagicMock()
    mock_response.read.return_value = b"<html></html>"
    mock_df = pd.read_csv("materials/message_check.csv")
    with patch('message_actions.urllib.urlopen') as mock_urlopen, \
         patch.dict('message_actions.messages_info_functions', {'BI': MagicMock()}):
        
        mock_urlopen.return_value = mock_response
        message_actions.messages_info_functions['BI'].return_value = mock_df

        assertExit(lambda: message_actions.extract_messages_from_topic(topic_row,ts_message_extract_min_utc,ts_message_extract_max_utc))

def test_extract_messages_from_topic_with_unmatching_timestamp():
    
    # this test the function extract_messages_from_topic with no messages dataframe. Must return a result None
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    ts_message_extract_min_utc = pd.Timestamp('3025-02-04 21:00:00')
    ts_message_extract_max_utc = pd.Timestamp('3025-02-04 22:30:00')

    mock_response = MagicMock()
    mock_response.read.return_value = b"<html></html>"
    mock_df = pd.read_csv("materials/message_check.csv")
    with patch('message_actions.urllib.urlopen') as mock_urlopen, \
         patch.dict('message_actions.messages_info_functions', {'BI': MagicMock()}):
        
        mock_urlopen.return_value = mock_response
        message_actions.messages_info_functions['BI'].return_value = mock_df

        result = message_actions.extract_messages_from_topic(topic_row,ts_message_extract_min_utc,ts_message_extract_max_utc)
        assert result is None

def test_extract_messages_min_ge_max():
    
    # this test the function extract_messages with a time range with min > max. The result must be an empty dataframe
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]

    mock_topics_scope_id = pd.read_csv("materials/qTopics_Calculate.csv")
    mock_ts_message_extract_min_utc = pd.Timestamp('2025-02-04 21:00:00')
    mock_ts_message_extract_max_utc = pd.Timestamp('1025-02-04 22:30:00')
    mock_results = pd.read_csv("materials/message_check.csv")

    with patch('message_actions.get_list_topics_from_need', return_value=mock_topics_scope_id), \
         patch('message_actions.get_extraction_time_range', return_value=(mock_ts_message_extract_min_utc,mock_ts_message_extract_max_utc)), \
         patch('message_actions.config.multithreading_run', return_value=[mock_results]), \
         patch('message_actions.create_csv'):

        df_messages, _ = message_actions.extract_messages(sr_snowflake_account, sr_output_need)
        unittest.TestCase().assertTrue(df_messages.empty)

def test_get_extraction_time_range_invalid_date():
    
    # this test the function get_extraction_time_range with invalid date. The result must be NaT
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_output_need = pd.read_csv("materials/edgecases/output_need_check_with_invalid_message_check_ts.csv").iloc[0]
    mock_df_time_max = pd.read_csv("materials/qGameDayEnd.csv")
    
    with patch('message_actions.snowflakeA.snowflake_execute', return_value = mock_df_time_max):
        min_ts, max_ts = message_actions.get_extraction_time_range(sr_snowflake_account, sr_output_need)
        unittest.TestCase().assertTrue(pd.isna(min_ts)) 
        unittest.TestCase().assertIsInstance(max_ts, pd.Timestamp)

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
    }),patch("message_actions.requests.Session") as mock_session:
        mock_sess_instance = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_sess_instance

        # Mock POST responses failed
        mock_sess_instance.get.return_value.text = "Login failed"
        mock_sess_instance.post.return_value.text = "Login failed"

        assertExit(lambda: message_actions.post_message_bi(topic_row, message_content,is_to_edit = 0))

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
         patch("os.getenv", return_value="http://fake-forum"):

        assertExit(lambda: message_actions.post_message_bi(topic_row, "timeout test",is_to_edit = 1))


if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_from_topic_invalid_timezone))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_from_topic_with_unmatching_timestamp))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_min_ge_max))
    test_suite.addTest(unittest.FunctionTestCase(test_get_extraction_time_range_invalid_date))
    test_suite.addTest(unittest.FunctionTestCase(test_post_message_bi_login_fail))
    test_suite.addTest(unittest.FunctionTestCase(test_post_message_bi_times_out))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)