'''
This tests file concern all functions in the output_actions module.
It units test unexpected path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from output_message_generation_actions import output_message_actions as output_message_actions
from testutils import assertExit
from testutils import read_json

def test_translate_string_missing_country_key():
    # this test the function translate_string with missing country key. Must exit the program.
    content = 'Hello __L__WEEKDAY_0__L____F__boldend__F__Goodbye'
    country = 'ITALIA'
    forum = 'BI'
    translations = read_json("materials/output_gameday_template_translations.json")
    assertExit(lambda: output_message_actions.translate_string(content, country, forum, translations))

def test_translate_string_invalid_type():
    
    # this test the function translate_param_for_country with invalid type (int instead of string). Must exit the program.
    content = 12345
    country = 'FRANCE'
    forum = 'BI'
    translations = read_json("materials/output_gameday_template_translations.json")
    assertExit(lambda: output_message_actions.translate_string(content, country, forum, translations))

def test_translate_df_headers_missing_country_key():
    
    # this test the function translate_df_headers with missing country key. Must exit the program.
    df = pd.DataFrame({'__D__USER_NAME__D__': ['USER1', 'USER2']})
    country = "ITALIA"
    forum = 'BI'
    translations = read_json("materials/output_gameday_template_translations.json")
    assertExit(lambda: output_message_actions.translate_df_headers(df, country, forum, translations))

def test_translate_df_headers_invalid_type():
    
    # this test the function translate_df_headers with invalid type (int instead of df). Must exit the program.
    df = 12345
    country = "FRANCE"
    forum = 'BI'
    translations = read_json("materials/output_gameday_template_translations.json")
    assertExit(lambda: output_message_actions.translate_df_headers(df, country, forum, translations))

def test_format_message_invalid_type():
    
    # this test the function format_message with invalid type (int instead of string). Must exit the program.
    message = 123
    assertExit(lambda: output_message_actions.format_message(message))

def test_format_message_invalid_pattern():
    
    # this test the function format_message with bad pattern. It must be accepted, without change.
    message = "Hello |X| world"
    result = output_message_actions.format_message(message)
    expected = "Hello |X| world"
    assert result == expected

def test_replace_conditionally_message_empty_text():
    
    # this test the function replace_conditionally_message with empty text. It must be return an empty text.
    output_text = ""
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = True
    result = output_message_actions.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = ""
    assert result == expected

def test_replace_conditionally_message_tags_missing():
    
    # this test the function replace_conditionally_message with missing tags. It must be accepted without changing text.
    output_text = "Hello World!"
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = True
    result = output_message_actions.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = "Hello World!"
    assert result == expected

def test_define_filename_missing_columns():
    
    # this test the function define_filename with missing columns (SEASON_ID for example). It must exit the program.
    input_type = "forumoutput_inited"
    sr_gameday_output_init = pd.read_csv("materials/edgecases/sr_gameday_output_init_noseasonid.csv").iloc[0]
    extension = "txt"
    country = "FRANCE"
    forum = "BI"
    assertExit(lambda: output_message_actions.define_filename(input_type, sr_gameday_output_init, extension, country, forum))

def test_define_filename_none_country():
    
    # this test the function define_filename with missing country. It must be accepted, and write a name without country
    input_type = "forumoutput_inited"
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    extension = "txt"
    country = None
    forum = "BI"
    result = output_message_actions.define_filename(input_type, sr_gameday_output_init, extension, country, forum)
    expected = 'forumoutput_inited_s1_1erejournee_bi.txt'
    assert result == expected

def test_display_rank_empty_df():
    
    # this test the function define_filename with empty dataframe. It must be accepted, and return an empty dataframe
    df = pd.DataFrame(columns=['rank'])
    result = output_message_actions.display_rank(df, 'rank')
    unittest.TestCase().assertTrue(result.empty)

def test_display_rank_duplicate_ranks():
    
    # this test the function define_filename with duplicate column rank. It must return a rank '-'
    df = pd.DataFrame({'rank': [1,1,2]})
    result = output_message_actions.display_rank(df, 'rank')
    unittest.TestCase().assertEqual(list(result['rank']), [1,'-',2])

def test_capture_df_oneheader_empty_df():
    
    # this test the function capture_df_oneheader with an empty df. It must exit the program.
    df = pd.DataFrame()
    capture_name = "test_capture.jpg"

    assertExit(lambda: output_message_actions.capture_df_oneheader(df, capture_name))

def test_capture_scores_detailed_empty_df():
    
    # this test the function capture_scores_detailed with empty dataframe. Must exit the program.
    df = pd.DataFrame()
    capture_name = "mycapture"

    with patch.object(output_message_actions.fileA, 'create_jpg'):
        assertExit(lambda: output_message_actions.capture_scores_detailed(df, capture_name))
    
def test_capture_scores_detailed_invalid_columns():
    
    # this test the function capture_scores_detailed with invalid columns in dataframe. Must exit the program.
    df = pd.DataFrame({"A":[1,2,3]})
    capture_name = "mycapture"

    with patch.object(output_message_actions.fileA, 'create_jpg'):
        assertExit(lambda: output_message_actions.capture_scores_detailed(df, capture_name))

def test_manage_df_empty_dataframe_raises():
    
    # this test the function manage_df with empty dataframe. Must exit the program.
    df = pd.DataFrame()
    country = "FRANCE"
    forum = "BI"
    capture_name = "capture"
    translations = read_json("materials/output_gameday_template_translations.json")
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]

    assertExit(lambda: output_message_actions.manage_df(
            df=df,
            country=country,
            forum=forum,
            capture_name=capture_name,
            sr_gameday_output=sr_gameday_output_calculate,
            translations_dict=translations
        ))

def test_manage_df_upload_failure_propagates():
    
    # this test the function manage_df with a forced error when uploading. Must exit the program.
    df = pd.DataFrame({"a": [1], "b": [2]})
    country = "FRANCE"
    forum = "BI"
    capture_name = "capture"
    translations = read_json("materials/output_gameday_template_translations.json")
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]


    with patch.object(output_message_actions,"translate_df_headers", return_value=df), \
         patch.object(output_message_actions,"define_filename", return_value="capture.jpg"), \
         patch.object(output_message_actions,"capture_df_oneheader"), \
         patch.object(output_message_actions,"push_capture_online", side_effect=RuntimeError("upload failed")), \
         patch.object(output_message_actions,"config") as mock_config:

        mock_config.TMPF = "/tmp"

        assertExit(lambda: output_message_actions.manage_df(
            df=df,
            country=country,
            forum=forum,
            capture_name=capture_name,
            sr_gameday_output=sr_gameday_output_calculate,
            translations_dict=translations
        ))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_translate_string_missing_country_key))
    test_suite.addTest(unittest.FunctionTestCase(test_translate_string_invalid_type))
    test_suite.addTest(unittest.FunctionTestCase(test_translate_df_headers_missing_country_key))
    test_suite.addTest(unittest.FunctionTestCase(test_translate_df_headers_invalid_type))
    test_suite.addTest(unittest.FunctionTestCase(test_format_message_invalid_type))
    test_suite.addTest(unittest.FunctionTestCase(test_format_message_invalid_pattern))
    test_suite.addTest(unittest.FunctionTestCase(test_replace_conditionally_message_empty_text))
    test_suite.addTest(unittest.FunctionTestCase(test_replace_conditionally_message_tags_missing))
    test_suite.addTest(unittest.FunctionTestCase(test_define_filename_missing_columns))
    test_suite.addTest(unittest.FunctionTestCase(test_define_filename_none_country))
    test_suite.addTest(unittest.FunctionTestCase(test_display_rank_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_display_rank_duplicate_ranks))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_df_oneheader_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_scores_detailed_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_scores_detailed_invalid_columns))
    test_suite.addTest(unittest.FunctionTestCase(test_manage_df_empty_dataframe_raises))
    test_suite.addTest(unittest.FunctionTestCase(test_manage_df_upload_failure_propagates))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)