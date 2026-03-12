'''
This tests file concern all functions in the output_message_generation module.
It units test the happy path for each function
'''
from unittest.mock import patch
import tempfile
import os
import pandas as pd

from src.predict_core.files_manipulation.local_files_manipulation.specific_files_operations.output_message_file_generation import output_message_generation
import src.predict_core.config.config_variables.config_global_variables as var

def test_translate_string(read_json):
    
    # this test the function translate_string
    content = 'Hello __L__WEEKDAY_0__L____F__boldend__F__Goodbye'
    country = 'FRANCE'
    forum = 'BI'
    translations = read_json("output_gameday_template_translations.json")
    result = output_message_generation.translate_string(content, country, forum, translations)
    assert result == 'Hello Dimanche[/b]Goodbye'

def test_translate_df_headers_one_level(read_json):
    
    # this test the function translate_param_for_country with a dataframe with one level header
    df = pd.DataFrame({'__D__USER_NAME__D__': ['USER1', 'USER2']})
    country = "FRANCE"
    forum = 'BI'
    translations = read_json("output_gameday_template_translations.json")
    expected = pd.DataFrame({'NOM': ['USER1', 'USER2']})
    result = output_message_generation.translate_df_headers(df, country, forum, translations)
    pd.testing.assert_frame_equal(result, expected)   

def test_translate_df_headers_two_level(read_json):
    
    # this test the function translate_param_for_country with a dataframe with two level header
    columns = pd.MultiIndex.from_tuples([('__D__USER_NAME__D__', ''),('Sales', 'Q2'),('', '__D__RANK__D__'),('__D__TOTAL_POINTS__D__', '__D__RANK__D__')])
    df = pd.DataFrame(data=[[100, 120, 30, 40],[90, 110, 25, 35]],columns=columns)
    country = "FRANCE"
    forum = 'BI'
    columns = pd.MultiIndex.from_tuples([('NOM', ''),('Sales', 'Q2'),('', 'CLSMT'),('TOTAL_POINTS', 'CLSMT')])
    translations = read_json("output_gameday_template_translations.json")
    expected = pd.DataFrame(data=[[100, 120, 30, 40],[90, 110, 25, 35]],columns=columns)
    result = output_message_generation.translate_df_headers(df, country, forum, translations)
    pd.testing.assert_frame_equal(result, expected)  

def test_format_message():
    
    # this test the function format_message 
    message = "Line1|2|\nLine2"
    result = output_message_generation.format_message(message)
    expected = "Line1\n\nLine2"
    assert result == expected

def test_replace_conditionally_message_true():
    
    # this test the function replace_conditionally_message with a condition met
    output_text = "Hello [START]World[END]!"
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = True
    result = output_message_generation.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = "Hello World!"
    assert result == expected

def test_replace_conditionally_message_false():
    
    # this test the function replace_conditionally_message with a condition not met
    output_text = "Hello [START]World[END]!"
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = False
    result = output_message_generation.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = "Hello !"
    assert result == expected

def test_define_filename(read_csv):

    # this test the function define_filename
    input_type = "forumoutput_inited"
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    extension = "txt"
    country = "FRANCE"
    forum = "BI"
    result = output_message_generation.define_filename(input_type, sr_gameday_output_init, extension, country, forum)
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

    result_df = output_message_generation.display_rank(df, 'RANK')
    
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_calculate_and_display_rank():
    
    # this test the function calculate_and_display_rank
    df = pd.DataFrame({'score':[10, 20, 20]})
    columns = ['score']
    result = output_message_generation.calculate_and_display_rank(df, columns)
    
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

    with patch.object(output_message_generation.files_manipulation,"create_jpg"): 
        output_message_generation.capture_df_oneheader(df, capture_name)

def test_capture_scores_detailed(read_csv):

    # this test the function capture_scores_detailed
    df = read_csv("output_message_calculated_scores_details.csv", header=[0, 1])
    capture_name = "mycapture"

    with patch.object(output_message_generation.files_manipulation, 'create_jpg'):
        output_message_generation.capture_scores_detailed(df, capture_name)

def test_manage_df(read_json, read_csv):
    
    # this test the function manage_df
    df = pd.DataFrame({"a": [1], "b": [2]})
    country = "FRANCE"
    forum = "BI"
    capture_name = "capture"
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]

    translated_df = df.copy()
    full_capture_name = "full_capture.jpg"
    translations = read_json("output_gameday_template_translations.json")
    expected_url = "https://example.com/full_capture.jpg"

    with patch.object(output_message_generation,"translate_df_headers", return_value=translated_df) as mock_translate, \
         patch.object(output_message_generation,"define_filename", return_value=full_capture_name) as mock_define_filename, \
         patch.object(output_message_generation,"capture_df_oneheader") as mock_capture_oneheader, \
         patch.object(output_message_generation,"capture_scores_detailed") as mock_capture_detailed, \
         patch.object(output_message_generation,"push_capture_online", return_value=expected_url) as mock_push:

        with tempfile.TemporaryDirectory() as tmp:
            var.TMPF = tmp

            result = output_message_generation.manage_df(
                df=df,
                country=country,
                forum=forum,
                capture_name=capture_name,
                sr_gameday_output=sr_gameday_output_calculate,
                translations_dict=translations
            )

            # Assert
            assert result == expected_url
            
            mock_translate.assert_called_once_with(df, country, forum,translations)
            mock_define_filename.assert_called_once_with(
                capture_name, sr_gameday_output_calculate, "jpg", country, forum
            )
            mock_capture_oneheader.assert_called_once_with(
                translated_df, full_capture_name
            )
            mock_capture_detailed.assert_not_called()
            expected_path = os.path.join(tmp, full_capture_name)
            mock_push.assert_called_once_with(expected_path)

def test_generate_output_message_init(read_csv):
    
    # this test the function generate_output_message - with INIT task
    context_dict = {
        "sr_output_need": read_csv("output_need_init.csv").iloc[0],
        "sr_snowflake_account_connect": read_csv("snowflake_account_connect.csv").iloc[0]
    }
    mock_df_gameday = read_csv("sr_gameday_output_init.csv").iloc[0]

    with patch.object(output_message_generation,"snowflake_execute", return_value= mock_df_gameday), \
         patch.object(output_message_generation.output_i,"process_output_message_inited") as mock_inited, \
         patch.object(output_message_generation.output_c,"process_output_message_calculated") as mock_calc:
        
        output_message_generation.generate_output_message(context_dict)
        mock_inited.assert_called_once()
        mock_calc.assert_not_called()

def test_generate_output_message_calculate(read_csv):
    
    # this test the function generate_output_message -  with CALCULATE task
    context_dict = {
        "sr_output_need": read_csv("output_need_calculate.csv").iloc[0],
        "sr_snowflake_account_connect": read_csv("snowflake_account_connect.csv").iloc[0]
    }
    mock_df_gameday = read_csv("sr_gameday_output_calculate.csv").iloc[0]

    with patch.object(output_message_generation,"snowflake_execute", return_value= mock_df_gameday), \
         patch.object(output_message_generation.output_i,"process_output_message_inited") as mock_inited, \
         patch.object(output_message_generation.output_c,"process_output_message_calculated") as mock_calc:
        
        output_message_generation.generate_output_message(context_dict)
        mock_calc.assert_called_once()
        mock_inited.assert_not_called()

