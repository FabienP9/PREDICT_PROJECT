'''
This tests file concern all functions in the messages_details_extraction module.
It units test unhappy paths
'''
import pandas as pd
from unittest.mock import MagicMock, patch

from src.predict_core.forums_interaction import messages_details_extraction

def test_extract_messages_from_topic_invalid_timezone(read_csv,assert_exit):
    
    # this test the function extract_messages_from_topic with topic row contains invalid timezone. Must exit program
    topic_row = read_csv("edgecases/q_topics_query_invalid_timezone.csv").itertuples(index=False)
    ts_message_extract_min_utc = pd.Timestamp('2025-02-04 21:00:00')
    ts_message_extract_max_utc = pd.Timestamp('2025-02-04 22:30:00')

    mock_response = MagicMock()
    mock_response.read.return_value = b"<html></html>"
    mock_df = read_csv("message_check.csv")
    with patch.object(messages_details_extraction.urllib,'urlopen') as mock_urlopen, \
         patch.dict(messages_details_extraction.messages_info_functions, {'BI': MagicMock()}):
        
        mock_urlopen.return_value = mock_response
        messages_details_extraction.messages_info_functions['BI'].return_value = mock_df

        assert_exit(lambda: messages_details_extraction.extract_messages_from_topic(topic_row,ts_message_extract_min_utc,ts_message_extract_max_utc))

def test_extract_messages_from_topic_with_unmatching_timestamp(read_csv):
    
    # this test the function extract_messages_from_topic with no messages dataframe. Must return a result None
    topic_row = next(read_csv("q_topics_query.csv").itertuples(index=False))
    ts_message_extract_min_utc = pd.Timestamp('3025-02-04 21:00:00')
    ts_message_extract_max_utc = pd.Timestamp('3025-02-04 22:30:00')

    mock_response = MagicMock()
    mock_response.read.return_value = b"<html></html>"
    mock_df = read_csv("message_check.csv")
    with patch.object(messages_details_extraction.urllib,'urlopen') as mock_urlopen, \
         patch.dict(messages_details_extraction.messages_info_functions, {'BI': MagicMock()}):
        
        mock_urlopen.return_value = mock_response
        messages_details_extraction.messages_info_functions['BI'].return_value = mock_df

        result = messages_details_extraction.extract_messages_from_topic(topic_row,ts_message_extract_min_utc,ts_message_extract_max_utc)
        assert result is None

def test_extract_messages_min_ge_max(read_yml_as_serie, read_csv):
    
    # this test the function extract_messages with a time range with min > max. The result must be an empty dataframe
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_output_need = read_csv("output_need_calculate.csv").iloc[0]

    mock_topics_scope_list = read_csv("q_topics_query.csv")
    mock_ts_message_extract_min_utc = pd.Timestamp('2025-02-04 21:00:00')
    mock_ts_message_extract_max_utc = pd.Timestamp('1025-02-04 22:30:00')
    mock_results = read_csv("message_check.csv")

    with patch.object(messages_details_extraction,'get_list_topics_from_need', return_value=mock_topics_scope_list), \
         patch.object(messages_details_extraction,'get_extraction_time_range', return_value=(mock_ts_message_extract_min_utc,mock_ts_message_extract_max_utc)), \
         patch.object(messages_details_extraction,'multithread_run', return_value=[mock_results]), \
         patch.object(messages_details_extraction,'create_csv'):

        df_messages, _ = messages_details_extraction.extract_messages(sr_snowflake_account_connect, sr_output_need)
        assert df_messages.empty

def test_get_extraction_time_range_invalid_date(read_yml_as_serie, read_csv):
    
    # this test the function get_extraction_time_range with invalid date. The result must be NaT
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_output_need = read_csv("edgecases/output_need_check_with_invalid_message_check_ts.csv").iloc[0]
    mock_df_time_max = read_csv("q_gameday_end_details_query.csv")
    
    with patch.object(messages_details_extraction,'snowflake_execute', return_value = mock_df_time_max):
        min_ts, max_ts = messages_details_extraction.get_extraction_time_range(sr_snowflake_account_connect, sr_output_need)
        assert pd.isna(min_ts) 
        assert isinstance(max_ts, pd.Timestamp)
