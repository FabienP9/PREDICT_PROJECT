'''
This tests file concern all functions in the get_messages_details_bi.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
from datetime import datetime
import pandas as pd
from pandas.testing import assert_frame_equal
import ast
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from message_actions.message_actions_bi import get_messages_details_bi as get_messages_details_bi
from testutils import read_txt

def test_translate_french_special_date_to_english():
    
    # this test the function translate_french_special_date_to_english
    date_string = "mar. 07 janv. 2025 9:54"
    expected = "Tue 07 Jan 2025 9:54"
    result = get_messages_details_bi.translate_french_special_date_to_english(date_string)
    assert result == expected

def test_transform_forum_time_to_datetime():
    
    # this test the function transform_forum_time_to_datetime
    date_string = "Mon 01 Jan 2025 9:08"
    result = get_messages_details_bi.transform_forum_time_to_datetime(date_string)
    assert isinstance(result, datetime)
    assert result.year == 2025 and result.month == 1 and result.day == 1
    assert result.hour == 9 and result.minute == 8

def test_get_users_bi():
    
    # this test the function get_users_bi
    message = read_txt("materials/bi_message_html.txt")
    expected = ast.literal_eval('[' + read_txt("materials/bi_get_users.txt") + ']')
    
    users = get_messages_details_bi.get_users_bi(message)
    assert users == expected

def test_get_ids_bi():
    
    # this test the function get_ids_bi
    message = read_txt("materials/bi_message_html.txt")
    expected = ast.literal_eval('[' + read_txt("materials/bi_get_ids.txt") + ']')
    
    ids = get_messages_details_bi.get_ids_bi(message)
    assert ids == expected

def test_get_creationtimes_bi():
    
    # this test the function get_creationtimes_bi
    message = read_txt("materials/bi_message_html.txt")
    expected = eval('[' + read_txt("materials/bi_get_creation_times.txt") + ']', {"datetime": datetime})

    creation_times = get_messages_details_bi.get_creationtimes_bi(message)
    assert creation_times == expected

def test_get_contents_outerblockquote_bi():
    
    # this test the function get_contents_outerblockquote_bi
    message = read_txt("materials/bi_message_html.txt")
    content = get_messages_details_bi.get_contents_outerblockquote_bi(message)
    expected_content = ast.literal_eval('[' + read_txt("materials/bi_get_content.txt") + ']')
    assert expected_content == content

def test_get_editiontimes_bi():

    # this test the function get_editiontimes_bi
    message = read_txt("materials/bi_message_html.txt")
    expected = eval('[' + read_txt("materials/bi_get_edition_times.txt") + ']', {"datetime": datetime})

    edition_times =  get_messages_details_bi.get_editiontimes_bi(message)
    assert edition_times == expected

def test_get_messages_details_bi():

    # this test the function get_messages_details_bi
    messagetext = read_txt("materials/bi_message_html.txt")
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    start = 0
    mock_users = ast.literal_eval('[' + read_txt("materials/bi_get_users.txt") + ']')
    mock_ids = ast.literal_eval('[' + read_txt("materials/bi_get_ids.txt") + ']')
    mock_creation_times = eval('[' + read_txt("materials/bi_get_creation_times.txt") + ']', {"datetime": datetime})
    mock_contents = ast.literal_eval('[' + read_txt("materials/bi_get_content.txt") + ']')
    mock_edition_times = eval('[' + read_txt("materials/bi_get_edition_times.txt") + ']', {"datetime": datetime})
    expected = pd.read_csv("materials/bi_get_messages_details.csv",quotechar='"')

    with patch.object(get_messages_details_bi,"get_users_bi", return_value = mock_users), \
         patch.object(get_messages_details_bi,"get_ids_bi", return_value = mock_ids), \
         patch.object(get_messages_details_bi,"get_creationtimes_bi", return_value = mock_creation_times), \
         patch.object(get_messages_details_bi,"get_contents_outerblockquote_bi", return_value = mock_contents), \
         patch.object(get_messages_details_bi,"get_editiontimes_bi", return_value = mock_edition_times):
    
        df_messages_infos = get_messages_details_bi.get_messages_details_bi(messagetext, topic_row, start)
        df_messages_infos['CREATION_TIME_LOCAL'] = pd.to_datetime(df_messages_infos['CREATION_TIME_LOCAL'])
        df_messages_infos['CREATION_TIME_LOCAL'] = df_messages_infos['CREATION_TIME_LOCAL'].dt.tz_localize(None)
        df_messages_infos['EDITION_TIME_LOCAL'] = pd.to_datetime(df_messages_infos['EDITION_TIME_LOCAL'])
        df_messages_infos['EDITION_TIME_LOCAL'] = df_messages_infos['EDITION_TIME_LOCAL'].dt.tz_localize(None)
        expected['CREATION_TIME_LOCAL'] = pd.to_datetime(expected['CREATION_TIME_LOCAL'])
        expected['CREATION_TIME_LOCAL'] = expected['CREATION_TIME_LOCAL'].dt.tz_localize(None)
        expected['EDITION_TIME_LOCAL'] = pd.to_datetime(expected['EDITION_TIME_LOCAL'])
        expected['EDITION_TIME_LOCAL'] = expected['EDITION_TIME_LOCAL'].dt.tz_localize(None)
        
        assert_frame_equal(df_messages_infos.reset_index(drop=True), expected.reset_index(drop=True),check_dtype=False)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_translate_french_special_date_to_english))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_forum_time_to_datetime))
    test_suite.addTest(unittest.FunctionTestCase(test_get_users_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_ids_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_creationtimes_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_contents_outerblockquote_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_editiontimes_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_messages_details_bi))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
