'''
This tests file concern all functions in the get_messages_details_bi.
It units test the unexpected path for each function, which return exception
'''
import unittest
from unittest.mock import patch
import pandas as pd
from datetime import datetime
import ast
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from message_actions.message_actions_bi import get_messages_details_bi as get_messages_details_bi
from testutils import read_txt

def test_get_users_empty_html():
    
    # this test the function get_users_bi without users. Must return empty user
    message = ""
    
    users = get_messages_details_bi.get_users_bi(message)
    assert users == []

def test_get_users_malformed_html():
    
    # this test the function get_users_bi without tags. Must return empty user
    message = "<span class='responsive-hide'><a>Missing class</a></span>"
    
    users = get_messages_details_bi.get_users_bi(message)
    assert users == []

def test_get_ids_malformed_ids():
    
    # this test the function get_ids_bi with id not int. Must raise the value (to the caller)
    message = "<dl class='postprofile' id='badprefix123'></dl>"
    unittest.TestCase().assertRaises(ValueError, get_messages_details_bi.get_ids_bi, message)

def test_get_ids_empty():
    
    # this test the function get_ids_bi with empty id. Must return an empty list
    message = ""
    
    ids = get_messages_details_bi.get_ids_bi(message)
    assert ids == []

def test_get_creationtimes_invalid_date():
    
    # this test the function get_creationtimes_bi with invalid date. Must raise the issue (to the caller)
    message = "<time>Invalid Date</time>"
    unittest.TestCase().assertRaises(ValueError, get_messages_details_bi.get_creationtimes_bi, message)

def test_content_outer_blockquote_nested():
    
    # this test the function get_contents_outerblockquote_bi with a lot of nested blocks. Must return the inside quote
    message = "<div class='content'><blockquote><blockquote>Nested</blockquote>Outer</blockquote></div>"
    expected = "<blockquote>NestedOuter</blockquote>"
    content = get_messages_details_bi.get_contents_outerblockquote_bi(message)
    assert content[0] == expected

def test_content_empty_div():
    
    # this test the function get_contents_outerblockquote_bi with an empty content. Must return an empty list
    message = ""
    content = get_messages_details_bi.get_contents_outerblockquote_bi(message)
    assert content == []

def test_editiontime_malformed_notice():
    
    # this test the function get_editiontimes_bi with a malformed date. Must return None
    message = "<div class='postbody'><div class='notice'>Modifié en dernier par Someone but missing date</div></div>"
    assert get_messages_details_bi.get_editiontimes_bi(message)[0] is None

def test_editiontime_no_notice():
    
    # this test the function get_editiontimes_bi with no edition
    message = read_txt("materials/edgecases/bi_message_html_noedition.txt")
    expected = eval('[' + read_txt("materials/edgecases/bi_get_edition_times_noedition.txt") + ']', {"datetime": datetime})

    edition_times =  get_messages_details_bi.get_editiontimes_bi(message)
    assert edition_times == expected

def test_get_messages_details_inconsistent_lengths():
    
    # this test the function get_messages_details_bi with inconsistent length between lists. Must raise an issue
    messagetext = read_txt("materials/bi_message_html.txt")
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    start = 0
    mock_users = ast.literal_eval('[' + read_txt("materials/bi_get_users.txt") + ']')
    mock_ids = ast.literal_eval('[' + read_txt("materials/bi_get_ids.txt") + ']')
    mock_creation_times = eval('[' + read_txt("materials/bi_get_creation_times.txt") + ']', {"datetime": datetime})
    mock_contents = ast.literal_eval('[' + read_txt("materials/bi_get_content.txt") + ']')
    mock_edition_times = eval('[' + read_txt("materials/bi_get_edition_times.txt") + ']', {"datetime": datetime})

    mock_creation_times = mock_creation_times[:-1] # we remove the last element from creation times so that the length is inconsistent with others

    with patch.object(get_messages_details_bi,"get_users_bi", return_value = mock_users), \
         patch.object(get_messages_details_bi,"get_ids_bi", return_value = mock_ids), \
         patch.object(get_messages_details_bi,"get_creationtimes_bi", return_value = mock_creation_times), \
         patch.object(get_messages_details_bi,"get_contents_outerblockquote_bi", return_value = mock_contents), \
         patch.object(get_messages_details_bi,"get_editiontimes_bi", return_value = mock_edition_times):
    
        unittest.TestCase().assertRaises(ValueError, get_messages_details_bi.get_messages_details_bi, messagetext, topic_row, start)

def test_get_messages_details_empty_html():
    
    # this test the function get_messages_details_bi with an empty text
    messagetext = ""
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    df = get_messages_details_bi.get_messages_details_bi(messagetext, topic_row, start=0)
    assert isinstance(df, pd.DataFrame)
    assert df.empty

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_users_empty_html))
    test_suite.addTest(unittest.FunctionTestCase(test_get_users_malformed_html))
    test_suite.addTest(unittest.FunctionTestCase(test_get_ids_malformed_ids))
    test_suite.addTest(unittest.FunctionTestCase(test_get_ids_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_get_creationtimes_invalid_date))
    test_suite.addTest(unittest.FunctionTestCase(test_content_outer_blockquote_nested))
    test_suite.addTest(unittest.FunctionTestCase(test_content_empty_div))
    test_suite.addTest(unittest.FunctionTestCase(test_editiontime_malformed_notice))
    test_suite.addTest(unittest.FunctionTestCase(test_editiontime_no_notice))
    test_suite.addTest(unittest.FunctionTestCase(test_get_messages_details_inconsistent_lengths))
    test_suite.addTest(unittest.FunctionTestCase(test_get_messages_details_empty_html))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
