'''
This tests file concern all functions in the output_message_generation module.
It units test unexpected path
'''
from unittest.mock import patch
import pandas as pd
import tempfile

from src.predict_core.files_manipulation.local_files_manipulation.specific_files_operations.output_message_file_generation import output_message_generation
import src.predict_core.config.config_variables.config_global_variables as var

def test_translate_string_missing_country_key(read_json, assert_exit):
    # this test the function translate_string with missing country key. Must exit the program.
    content = 'Hello __L__WEEKDAY_0__L____F__boldend__F__Goodbye'
    country = 'ITALIA'
    forum = 'BI'
    translations = read_json("output_gameday_template_translations.json")
    assert_exit(lambda: output_message_generation.translate_string(content, country, forum, translations))

def test_translate_string_invalid_type(read_json, assert_exit):
    
    # this test the function translate_param_for_country with invalid type (int instead of string). Must exit the program.
    content = 12345
    country = 'FRANCE'
    forum = 'BI'
    translations = read_json("output_gameday_template_translations.json")
    assert_exit(lambda: output_message_generation.translate_string(content, country, forum, translations))

def test_translate_df_headers_missing_country_key(read_json, assert_exit):
    
    # this test the function translate_df_headers with missing country key. Must exit the program.
    df = pd.DataFrame({'__D__USER_NAME__D__': ['USER1', 'USER2']})
    country = "ITALIA"
    forum = 'BI'
    translations = read_json("output_gameday_template_translations.json")
    assert_exit(lambda: output_message_generation.translate_df_headers(df, country, forum, translations))

def test_translate_df_headers_invalid_type(read_json, assert_exit):
    
    # this test the function translate_df_headers with invalid type (int instead of df). Must exit the program.
    df = 12345
    country = "FRANCE"
    forum = 'BI'
    translations = read_json("output_gameday_template_translations.json")
    assert_exit(lambda: output_message_generation.translate_df_headers(df, country, forum, translations))

def test_format_message_invalid_type(assert_exit):
    
    # this test the function format_message with invalid type (int instead of string). Must exit the program.
    message = 123
    assert_exit(lambda: output_message_generation.format_message(message))

def test_format_message_invalid_pattern():
    
    # this test the function format_message with bad pattern. It must be accepted, without change.
    message = "Hello |X| world"
    result = output_message_generation.format_message(message)
    expected = "Hello |X| world"
    assert result == expected

def test_replace_conditionally_message_empty_text():
    
    # this test the function replace_conditionally_message with empty text. It must be return an empty text.
    output_text = ""
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = True
    result = output_message_generation.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = ""
    assert result == expected

def test_replace_conditionally_message_tags_missing():
    
    # this test the function replace_conditionally_message with missing tags. It must be accepted without changing text.
    output_text = "Hello World!"
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = True
    result = output_message_generation.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = "Hello World!"
    assert result == expected

def test_define_filename_missing_columns(read_csv, assert_exit):
    
    # this test the function define_filename with missing columns (SEASON_ID for example). It must exit the program.
    input_type = "forumoutput_inited"
    sr_gameday_output_init = read_csv("edgecases/sr_gameday_output_init_noseasonid.csv").iloc[0]
    extension = "txt"
    country = "FRANCE"
    forum = "BI"
    assert_exit(lambda: output_message_generation.define_filename(input_type, sr_gameday_output_init, extension, country, forum))

def test_define_filename_none_country(read_csv):
    
    # this test the function define_filename with missing country. It must be accepted, and write a name without country
    input_type = "forumoutput_inited"
    sr_gameday_output_init = read_csv("sr_gameday_output_init.csv").iloc[0]
    extension = "txt"
    country = None
    forum = "BI"
    result = output_message_generation.define_filename(input_type, sr_gameday_output_init, extension, country, forum)
    expected = 'forumoutput_inited_s1_1erejournee_bi.txt'
    assert result == expected

def test_display_rank_empty_df():
    
    # this test the function define_filename with empty dataframe. It must be accepted, and return an empty dataframe
    df = pd.DataFrame(columns=['rank'])
    result = output_message_generation.display_rank(df, 'rank')
    assert result.empty

def test_display_rank_duplicate_ranks():
    
    # this test the function define_filename with duplicate column rank. It must return a rank '-'
    df = pd.DataFrame({'rank': [1,1,2]})
    result = output_message_generation.display_rank(df, 'rank')
    assert list(result['rank']) == [1,'-',2]

def test_capture_df_oneheader_empty_df(assert_exit):
    
    # this test the function capture_df_oneheader with an empty df. It must exit the program.
    df = pd.DataFrame()
    capture_name = "test_capture.jpg"

    assert_exit(lambda: output_message_generation.capture_df_oneheader(df, capture_name))

def test_capture_scores_detailed_empty_df(assert_exit):
    
    # this test the function capture_scores_detailed with empty dataframe. Must exit the program.
    df = pd.DataFrame()
    capture_name = "mycapture"

    with patch.object(output_message_generation.files_manipulation, 'create_jpg'):
        assert_exit(lambda: output_message_generation.capture_scores_detailed(df, capture_name))
    
def test_capture_scores_detailed_invalid_columns(assert_exit):
    
    # this test the function capture_scores_detailed with invalid columns in dataframe. Must exit the program.
    df = pd.DataFrame({"A":[1,2,3]})
    capture_name = "mycapture"

    with patch.object(output_message_generation.files_manipulation, 'create_jpg'):
        assert_exit(lambda: output_message_generation.capture_scores_detailed(df, capture_name))

def test_manage_df_empty_dataframe_raises(read_json, read_csv, assert_exit):
    
    # this test the function manage_df with empty dataframe. Must exit the program.
    df = pd.DataFrame()
    country = "FRANCE"
    forum = "BI"
    capture_name = "capture"
    translations = read_json("output_gameday_template_translations.json")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]

    assert_exit(lambda: output_message_generation.manage_df(
            df=df,
            country=country,
            forum=forum,
            capture_name=capture_name,
            sr_gameday_output=sr_gameday_output_calculate,
            translations_dict=translations
        ))

def test_manage_df_upload_failure_propagates(read_json, read_csv, assert_exit):
    
    # this test the function manage_df with a forced error when uploading. Must exit the program.
    df = pd.DataFrame({"a": [1], "b": [2]})
    country = "FRANCE"
    forum = "BI"
    capture_name = "capture"
    translations = read_json("output_gameday_template_translations.json")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]


    with patch.object(output_message_generation,"translate_df_headers", return_value=df), \
         patch.object(output_message_generation,"define_filename", return_value="capture.jpg"), \
         patch.object(output_message_generation,"capture_df_oneheader"), \
         patch.object(output_message_generation,"push_capture_online", side_effect=RuntimeError("upload failed")):

        with tempfile.TemporaryDirectory() as tmp:
            var.TMPF = tmp

            assert_exit(lambda: output_message_generation.manage_df(
                df=df,
                country=country,
                forum=forum,
                capture_name=capture_name,
                sr_gameday_output=sr_gameday_output_calculate,
                translations_dict=translations
            ))
