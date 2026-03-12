'''
This tests file concern all functions in the messages_details_extraction_bi.
It units test the unexpected path
'''
import pytest
import ast
from datetime import datetime
from unittest.mock import patch
from bs4 import BeautifulSoup as bs
import pandas as pd

from src.predict_core.forums_interaction.forums_interaction_bi import messages_details_extraction_bi
def test_get_users_empty_html():
    
    # this test the function get_users_bi without users. Must return empty user
    message = ""
    soup = bs(message, "html.parser")
    users = messages_details_extraction_bi.get_users_bi(soup)
    assert users == []

def test_get_users_malformed_html():
    
    # this test the function get_users_bi without tags. Must return empty user
    message = "<span class='responsive-hide'><a>Missing class</a></span>"
    soup = bs(message, "html.parser")
    users = messages_details_extraction_bi.get_users_bi(soup)
    assert users == []

def test_get_ids_malformed_ids():
    
    # this test the function get_ids_bi with id not int. Must raise the value (to the caller)
    message = "<dl class='postprofile' id='badprefix123'></dl>"
    soup = bs(message, "html.parser")
    with pytest.raises(ValueError):
        messages_details_extraction_bi.get_ids_bi(soup)

def test_get_ids_empty():
    
    # this test the function get_ids_bi with empty id. Must return an empty list
    message = ""
    soup = bs(message, "html.parser")    
    ids = messages_details_extraction_bi.get_ids_bi(soup)
    assert ids == []

def test_get_creationtimes_invalid_date():
    
    # this test the function get_creationtimes_bi with invalid date. Must raise the issue (to the caller)
    message = "<time>Invalid Date</time>"
    soup = bs(message, "html.parser")
    with pytest.raises(ValueError):
        messages_details_extraction_bi.get_creationtimes_bi(soup)
    
def test_content_outer_blockquote_nested():
    
    # this test the function get_contents_outerblockquote_bi with a lot of nested blocks. Must return the inside quote
    message = "<div class='content'><blockquote><blockquote>Nested</blockquote>Outer</blockquote></div>"
    soup = bs(message, "html.parser")
    expected = "<blockquote>NestedOuter</blockquote>"
    content = messages_details_extraction_bi.get_contents_outerblockquote_bi(soup)
    assert content[0] == expected

def test_content_empty_div():
    
    # this test the function get_contents_outerblockquote_bi with an empty content. Must return an empty list
    message = ""
    soup = bs(message, "html.parser")
    content = messages_details_extraction_bi.get_contents_outerblockquote_bi(soup)
    assert content == []

def test_editiontime_malformed_notice():
    
    # this test the function get_editiontimes_bi with a malformed date. Must return None
    message = "<div class='postbody'><div class='notice'>Modifié en dernier par Someone but missing date</div></div>"
    soup = bs(message, "html.parser")
    assert messages_details_extraction_bi.get_editiontimes_bi(soup)[0] is None

def test_editiontime_no_notice(read_txt):
    
    # this test the function get_editiontimes_bi with no edition
    message = read_txt("edgecases/bi_message_html_noedition.txt")
    soup = bs(message, "html.parser")
    expected = eval('[' + read_txt("edgecases/bi_get_edition_times_noedition.txt") + ']', {"datetime": datetime})

    edition_times =  messages_details_extraction_bi.get_editiontimes_bi(soup)
    assert edition_times == expected

def test_get_messages_details_inconsistent_lengths(read_txt, read_csv):
    
    # this test the function get_messages_details_bi with inconsistent length between lists. Must raise an issue
    messagetext = read_txt("bi_message_html.txt")
    topic_row = next(read_csv("q_topics_query.csv").itertuples(index=False))
    start = 0
    mock_users = ast.literal_eval('[' + read_txt("bi_get_users.txt") + ']')
    mock_ids = ast.literal_eval('[' + read_txt("bi_get_ids.txt") + ']')
    mock_creation_times = eval('[' + read_txt("bi_get_creation_times.txt") + ']', {"datetime": datetime})
    mock_contents = ast.literal_eval('[' + read_txt("bi_get_content.txt") + ']')
    mock_edition_times = eval('[' + read_txt("bi_get_edition_times.txt") + ']', {"datetime": datetime})

    mock_creation_times = mock_creation_times[:-1] # we remove the last element from creation times so that the length is inconsistent with others

    with patch.object(messages_details_extraction_bi,"get_users_bi", return_value = mock_users), \
         patch.object(messages_details_extraction_bi,"get_ids_bi", return_value = mock_ids), \
         patch.object(messages_details_extraction_bi,"get_creationtimes_bi", return_value = mock_creation_times), \
         patch.object(messages_details_extraction_bi,"get_contents_outerblockquote_bi", return_value = mock_contents), \
         patch.object(messages_details_extraction_bi,"get_editiontimes_bi", return_value = mock_edition_times):
      
        with pytest.raises(ValueError):
            messages_details_extraction_bi.get_messages_details_bi(messagetext, topic_row, start)

def test_get_messages_details_empty_html(read_csv):
    
    # this test the function get_messages_details_bi with an empty text
    messagetext = ""
    topic_row = next(read_csv("q_topics_query.csv").itertuples(index=False))
    df = messages_details_extraction_bi.get_messages_details_bi(messagetext, topic_row, start=0)
    assert isinstance(df, pd.DataFrame)
    assert df.empty

