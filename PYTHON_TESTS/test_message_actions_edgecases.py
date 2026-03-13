'''
This tests file concern all functions in the message_actions module.
It units test unhappy paths for each function
'''
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from message_actions import message_actions as message_actions
from testutils import assertExit

def test_extract_messages_from_topic_invalid_timezone():
    
    # this test the function extract_messages_from_topic with topic row contains invalid timezone. Must exit program
    topic_row = pd.read_csv("materials/edgecases/qTopics_Calculate_invalid_timezone.csv").iloc[0]
    ts_message_extract_min_utc = pd.Timestamp('2025-02-04 21:00:00')
    ts_message_extract_max_utc = pd.Timestamp('2025-02-04 22:30:00')

    mock_response = MagicMock()
    mock_response.read.return_value = b"<html></html>"
    mock_df = pd.read_csv("materials/message_check.csv")
    with patch.object(message_actions.urllib,'urlopen') as mock_urlopen, \
         patch.dict(message_actions.messages_info_functions, {'BI': MagicMock()}):
        
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
    with patch.object(message_actions.urllib,'urlopen') as mock_urlopen, \
         patch.dict(message_actions.messages_info_functions, {'BI': MagicMock()}):
        
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

    with patch.object(message_actions,'get_list_topics_from_need', return_value=mock_topics_scope_id), \
         patch.object(message_actions,'get_extraction_time_range', return_value=(mock_ts_message_extract_min_utc,mock_ts_message_extract_max_utc)), \
         patch.object(message_actions.config,'multithreading_run', return_value=[mock_results]), \
         patch.object(message_actions,'create_csv'):

        df_messages, _ = message_actions.extract_messages(sr_snowflake_account, sr_output_need)
        unittest.TestCase().assertTrue(df_messages.empty)

def test_get_extraction_time_range_invalid_date():
    
    # this test the function get_extraction_time_range with invalid date. The result must be NaT
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_output_need = pd.read_csv("materials/edgecases/output_need_check_with_invalid_message_check_ts.csv").iloc[0]
    mock_df_time_max = pd.read_csv("materials/message_actions_qGameDayEnd.csv")
    
    with patch.object(message_actions.snowflakeA,'snowflake_execute', return_value = mock_df_time_max):
        min_ts, max_ts = message_actions.get_extraction_time_range(sr_snowflake_account, sr_output_need)
        unittest.TestCase().assertTrue(pd.isna(min_ts)) 
        unittest.TestCase().assertIsInstance(max_ts, pd.Timestamp)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_from_topic_invalid_timezone))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_from_topic_with_unmatching_timestamp))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_min_ge_max))
    test_suite.addTest(unittest.FunctionTestCase(test_get_extraction_time_range_invalid_date))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)