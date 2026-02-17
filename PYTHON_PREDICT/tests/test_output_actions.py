'''
This tests file concern all functions in the output_actions module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import output_actions

def test_translate_string():
    
    # this test the function translate_string
    content = 'Hello __L__WEEKDAY_0__L____F__boldend__F__Goodbye'
    country = 'FRANCE'
    forum = 'BI'
    result = output_actions.translate_string(content, country, forum)
    assert result == 'Hello Dimanche[/b]Goodbye'

def test_translate_df_headers_one_level():
    
    # this test the function translate_param_for_country with a dataframe with one level header
    df = pd.DataFrame({'__D__USER_NAME__D__': ['USER1', 'USER2']})
    country = "FRANCE"
    forum = 'BI'
    expected = pd.DataFrame({'NOM': ['USER1', 'USER2']})
    result = output_actions.translate_df_headers(df, country, forum)
    pd.testing.assert_frame_equal(result, expected)   

def test_translate_df_headers_two_level():
    
    # this test the function translate_param_for_country with a dataframe with two level header
    columns = pd.MultiIndex.from_tuples([('__D__USER_NAME__D__', ''),('Sales', 'Q2'),('', '__D__RANK__D__'),('__D__TOTAL_POINTS__D__', '__D__RANK__D__')])
    df = pd.DataFrame(data=[[100, 120, 30, 40],[90, 110, 25, 35]],columns=columns)
    country = "FRANCE"
    forum = 'BI'
    columns = pd.MultiIndex.from_tuples([('NOM', ''),('Sales', 'Q2'),('', 'CLSMT'),('TOTAL_POINTS', 'CLSMT')])
    expected = pd.DataFrame(data=[[100, 120, 30, 40],[90, 110, 25, 35]],columns=columns)
    result = output_actions.translate_df_headers(df, country, forum)
    pd.testing.assert_frame_equal(result, expected)  

def test_format_message():
    
    # this test the function format_message 
    message = "Line1|2|\nLine2"
    result = output_actions.format_message(message)
    expected = "Line1\n\nLine2"
    assert result == expected

def test_replace_conditionally_message_true():
    
    # this test the function replace_conditionally_message with a condition met
    output_text = "Hello [START]World[END]!"
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = True
    result = output_actions.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = "Hello World!"
    assert result == expected

def test_replace_conditionally_message_false():
    
    # this test the function replace_conditionally_message with a condition not met
    output_text = "Hello [START]World[END]!"
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = False
    result = output_actions.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = "Hello !"
    assert result == expected

def test_define_filename():

    # this test the function define_filename
    input_type = "forumoutput_inited"
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    extension = "txt"
    country = "FRANCE"
    forum = "BI"
    result = output_actions.define_filename(input_type, sr_gameday_output_init, extension, country, forum)
    expected = 'forumoutput_inited_s1_1erejournee_france_bi.txt'
    assert result == expected

def test_display_rank():
    
    # this test the function display_rank
    df = pd.DataFrame({
        'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'RANK': [4, 2, 1, 2]
    })
    
    expected_df = pd.DataFrame({
        'RANK': [1, 2, '-', 4],
        'Name': ['Charlie', 'Bob', 'David', 'Alice']
    })

    result_df = output_actions.display_rank(df, 'RANK')
    
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_calculate_and_display_rank():
    
    # this test the function calculate_and_display_rank
    df = pd.DataFrame({'score':[10, 20, 20]})
    columns = ['score']
    result = output_actions.calculate_and_display_rank(df, columns)
    
    result['RANK'] = result['RANK'].astype(str)
    expected_df = pd.DataFrame({
        'RANK': ['1','-','3'],
        'score': [20,20,10]
    })
    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_capture_df_oneheader():
    
    # this test the function capture_df_oneheader
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": [3, 4]
    })
    capture_name = "test_capture.jpg"

    with patch("output_actions.fileA.create_jpg"): 
        output_actions.capture_df_oneheader(df, capture_name)

def test_capture_scores_detailed():

    # this test the function capture_scores_detailed
    df = pd.read_csv("materials/output_actions_calculated_scores_details.csv", header=[0, 1])
    capture_name = "mycapture"

    with patch.object(output_actions.fileA, 'create_jpg'):
        output_actions.capture_scores_detailed(df, capture_name)

def test_manage_df():
    
    # this test the function manage_df
    df = pd.DataFrame({"a": [1], "b": [2]})
    country = "FRANCE"
    forum = "BI"
    capture_name = "capture"
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]

    translated_df = df.copy()
    full_capture_name = "full_capture.jpg"
    expected_url = "https://example.com/full_capture.jpg"

    with patch("output_actions.translate_df_headers", return_value=translated_df) as mock_translate, \
         patch("output_actions.define_filename", return_value=full_capture_name) as mock_define_filename, \
         patch("output_actions.capture_df_oneheader") as mock_capture_oneheader, \
         patch("output_actions.capture_scores_detailed") as mock_capture_detailed, \
         patch("output_actions.push_capture_online", return_value=expected_url) as mock_push, \
         patch("output_actions.config") as mock_config:

        mock_config.TMPF = "/tmp"

        result = output_actions.manage_df(
            df=df,
            country=country,
            forum=forum,
            capture_name=capture_name,
            sr_gameday_output=sr_gameday_output_calculate,
        )

        # Assert
        assert result == expected_url

        mock_translate.assert_called_once_with(df, country, forum)
        mock_define_filename.assert_called_once_with(
            capture_name, sr_gameday_output_calculate, "jpg", country, forum
        )
        mock_capture_oneheader.assert_called_once_with(
            translated_df, full_capture_name
        )
        mock_capture_detailed.assert_not_called()
        mock_push.assert_called_once_with("/tmp/full_capture.jpg")

def test_generate_output_message_init():
    
    # this test the function generate_output_message - with INIT task
    context_dict = {
        "sr_output_need": pd.read_csv("materials/output_need_init.csv").iloc[0],
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    }
    mock_df_gameday_output = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]

    with patch("output_actions.snowflake_execute", return_value= mock_df_gameday_output), \
         patch("output_actions.outputAI.process_output_message_inited") as mock_inited, \
         patch("output_actions.outputAC.process_output_message_calculated") as mock_calc:
        
        output_actions.generate_output_message(context_dict)
        mock_inited.assert_called_once()
        mock_calc.assert_not_called()

def test_generate_output_message_calculate():
    
    # this test the function generate_output_message -  with CALCULATE task
    context_dict = {
        "sr_output_need": pd.read_csv("materials/output_need_calculate.csv").iloc[0],
        "sr_snowflake_account_connect": pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    }
    mock_df_gameday_output = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]

    with patch("output_actions.snowflake_execute", return_value= mock_df_gameday_output), \
         patch("output_actions.outputAI.process_output_message_inited") as mock_inited, \
         patch("output_actions.outputAC.process_output_message_calculated") as mock_calc:
        
        output_actions.generate_output_message(context_dict)
        mock_calc.assert_called_once()
        mock_inited.assert_not_called()

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_translate_string))
    test_suite.addTest(unittest.FunctionTestCase(test_translate_df_headers_one_level))
    test_suite.addTest(unittest.FunctionTestCase(test_translate_df_headers_two_level))
    test_suite.addTest(unittest.FunctionTestCase(test_format_message))
    test_suite.addTest(unittest.FunctionTestCase(test_replace_conditionally_message_true))
    test_suite.addTest(unittest.FunctionTestCase(test_replace_conditionally_message_false))
    test_suite.addTest(unittest.FunctionTestCase(test_define_filename))
    test_suite.addTest(unittest.FunctionTestCase(test_display_rank))
    test_suite.addTest(unittest.FunctionTestCase(test_calculate_and_display_rank))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_df_oneheader))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_scores_detailed))
    test_suite.addTest(unittest.FunctionTestCase(test_manage_df))
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_message_init))
    test_suite.addTest(unittest.FunctionTestCase(test_generate_output_message_calculate))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
