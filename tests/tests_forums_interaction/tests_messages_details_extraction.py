'''
This tests file concern all functions in the messages_details_extraction module.
It units test the happy path for each function
'''

from unittest.mock import MagicMock, patch
from pandas.testing import assert_frame_equal
import pandas as pd

from src.predict_core.forums_interaction import messages_details_extraction

def test_extract_messages_from_topic(read_csv):
    
    # this test the function extract_messages_from_topic
    topic_row = next(read_csv("q_topics_query.csv").itertuples(index=False))
    ts_message_extract_min_utc = pd.Timestamp('2025-02-04 21:00:00')
    ts_message_extract_max_utc = pd.Timestamp('2025-02-04 22:30:00')

    mock_response = MagicMock()
    mock_response.read.return_value = b"<html></html>"
    mock_df = read_csv("message_check.csv")
    with patch.object(messages_details_extraction.urllib,'urlopen') as mock_urlopen, \
         patch.dict(messages_details_extraction.messages_info_functions, {'BI': MagicMock()}):
        
        mock_urlopen.return_value = mock_response
        messages_details_extraction.messages_info_functions['BI'].return_value = mock_df

        result = messages_details_extraction.extract_messages_from_topic(topic_row,ts_message_extract_min_utc,ts_message_extract_max_utc)
        assert_frame_equal(result.reset_index(drop=True), mock_df.iloc[[1]].reset_index(drop=True))

def test_extract_messages(read_csv):

    # this test the function extract_messages
    sr_snowflake_account = read_csv("snowflake_account_connect.csv").iloc[0]
    sr_output_need = read_csv("output_need_calculate.csv").iloc[0]

    mock_topics_scope_id = read_csv("q_topics_query.csv")
    mock_ts_message_extract_min_utc = pd.Timestamp('2025-02-04 21:00:00')
    mock_ts_message_extract_max_utc = pd.Timestamp('2025-02-04 22:30:00')
    mock_results = read_csv("message_check.csv")

    with patch.object(messages_details_extraction,'get_list_topics_from_need', return_value=mock_topics_scope_id), \
         patch.object(messages_details_extraction,'get_extraction_time_range', return_value=(mock_ts_message_extract_min_utc,mock_ts_message_extract_max_utc)), \
         patch.object(messages_details_extraction,'multithread_run', return_value=[mock_results]), \
         patch.object(messages_details_extraction,'create_csv'):

        df_messages, ts_message_extract_max_utc = messages_details_extraction.extract_messages(sr_snowflake_account, sr_output_need)
        assert_frame_equal(df_messages.reset_index(drop=True), mock_results.reset_index(drop=True))
        assert ts_message_extract_max_utc == mock_ts_message_extract_max_utc

def test_get_list_topics_from_need(read_csv):
    
    # this test the function get_list_topics_from_need
    sr_snowflake_account = read_csv("snowflake_account_connect.csv").iloc[0]
    sr_output_need = read_csv("output_need_calculate.csv").iloc[0]
    mock_topics_scope_list = read_csv("q_topics_query.csv")
    
    with patch.object(messages_details_extraction,'snowflake_execute', return_value=mock_topics_scope_list):
    
        df_topics = messages_details_extraction.get_list_topics_from_need(sr_snowflake_account, sr_output_need)
        assert_frame_equal(df_topics.reset_index(drop=True), mock_topics_scope_list.reset_index(drop=True))

def test_get_extraction_time_range(read_csv):
    
    # this test the function get_extraction_time_range
    sr_snowflake_account = read_csv("snowflake_account_connect.csv").iloc[0]
    sr_output_need = read_csv("output_need_check_with_message_check_ts.csv").iloc[0]
    mock_df_time_max = read_csv("q_gameday_end_details_query.csv")
    
    with patch.object(messages_details_extraction,'snowflake_execute', return_value = mock_df_time_max):
        min_ts, max_ts = messages_details_extraction.get_extraction_time_range(sr_snowflake_account, sr_output_need)
        assert min_ts == pd.Timestamp('2025-06-14 15:00:00')
        assert max_ts == pd.Timestamp('2025-10-31 18:30:00')