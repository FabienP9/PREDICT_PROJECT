'''
This tests file concern all functions in the main module.
It units test the happy path for each function
'''
from unittest.mock import patch,MagicMock
from pandas.testing import assert_frame_equal
import pandas as pd

from src.predict_core.entry_point import main

def test_process_games(read_csv):
    
    # this test the process_games function
    context = {
        'sr_output_need': read_csv("output_need_calculate.csv").iloc[0],
        'df_competition': read_csv("competition_unique.csv"),
        "df_paths" : read_csv("paths.csv"),
        "df_gameday_modification":pd.DataFrame()
    }

    mock_df_game = read_csv("game.csv")

    with patch.object(main.games_details_extraction,"extract_games_from_need",return_value=mock_df_game), \
         patch.object(main.files_manipulation,"filter_data"):

        result = main.process_games(context)
        assert "df_game" in result

def test_process_messages_autoprocess(read_yml_as_serie, read_csv):
    
    # this test the process_messages function with an automatic process
    context = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'sr_output_need': read_csv("output_need_check_with_message_check_ts.csv").iloc[0],
        "df_paths" : read_csv("paths.csv"),
        "df_boolean_check_message_manually" : read_csv("boolean_check_message_manually_0.csv"),
        "df_message_check_ts" : read_csv("message_check_ts.csv"),      
    }
    df_message_check = read_csv("message_check.csv")
    extraction_time_utc = '2025-01-01 18:00:00'

    with patch.object(main.messages_details_extraction,"extract_messages",return_value=(df_message_check,extraction_time_utc)), \
         patch.object(main.files_manipulation,"filter_data", return_value={}), \
         patch.object(main.output_need_calculation,"set_output_need_to_check_status"), \
         patch.object(main.files_manipulation,"create_csv"), \
         patch.object(main.files_manipulation,"create_csv"):

            result = main.process_messages(context)
            assert "df_message_check" in result
            assert "extraction_time_utc" in result
            assert "df_message_check_ts" in result
            assert (result['df_message_check_ts'].loc[result['df_message_check_ts']['SEASON_ID'] == context['sr_output_need']['SEASON_ID'], 'LAST_CHECK_TS_UTC'] == extraction_time_utc).all()

def test_process_messages_manualprocess(read_yml_as_serie, read_csv):
    
    # this test the process_messages function with an manual process
    context = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'sr_output_need': read_csv("output_need_check_with_message_check_ts.csv").iloc[0],
        "df_paths" : read_csv("paths.csv"),
        "df_boolean_check_message_manually" : read_csv("boolean_check_message_manually_1.csv"),
        "df_message_check_ts" : read_csv("message_check_ts.csv"),      
    }
    df_message_check = read_csv("message_check.csv")
    extraction_time_utc = '2025-01-01 18:00:00'

    with patch.object(main.messages_details_extraction,"extract_messages",return_value=(df_message_check,extraction_time_utc)), \
         patch.object(main.files_manipulation,"filter_data", return_value={}), \
         patch.object(main.output_need_calculation,"set_output_need_to_check_status"), \
         patch.object(main.files_manipulation,"create_csv"), \
         patch.object(main.files_manipulation,"create_csv"):

            result = main.process_messages(context)
            assert "df_message_check" in result
            assert "extraction_time_utc" in result
            assert "df_message_check_ts" in result
            assert_frame_equal(result['df_message_check_ts'].reset_index(drop=True), context['df_message_check_ts'].reset_index(drop=True))

def test_display_check_string_with_messages(read_yml_as_serie, read_csv):
    
    # this test the display_check_string function with messages - manual processing
    context = {
        'sr_output_need': read_csv("output_need_calculate.csv").iloc[0],
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'extraction_time_utc':  "2000-01-02 01:00:00",
        'nb_new_messages': 3,
        "df_boolean_check_message_manually" : read_csv("boolean_check_message_manually_1.csv")
    }
    context['sr_output_need']['TASK_RUN']  = "CHECK"
    context['sr_output_need']['LAST_MESSAGE_CHECK_TS_UTC']  = "2000-01-01 00:01:00"
    check_string = main.display_check_string(context)
    assert check_string.strip() != ""
    assert check_string is not None
    assert check_string != "No need to check - no new messages"

def test_display_check_string_without_messages(read_csv, read_yml_as_serie):
    
    # this test the display_check_string function without messages - manual processing
    context = {
        'sr_output_need': read_csv("output_need_calculate.csv").iloc[0],
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'extraction_time_utc':  "2000-01-02 01:00:00",
        'nb_new_messages': 0,
        "df_boolean_check_message_manually" : read_csv("boolean_check_message_manually_1.csv")
    }
    context['sr_output_need']['TASK_RUN']  = "CHECK"
    context['sr_output_need']['LAST_MESSAGE_CHECK_TS_UTC']  = "2000-01-01 00:01:00"
    check_string = main.display_check_string(context)
    assert check_string == "No need to check - no new messages"

def test_display_check_string_autoprocess(read_csv, read_yml_as_serie):
    
    # this test the display_check_string function with auto processing
    context = {
        'sr_output_need': read_csv("output_need_calculate.csv").iloc[0],
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'extraction_time_utc':  "2000-01-02 01:00:00",
        'nb_new_messages': 0,
        "df_boolean_check_message_manually" : read_csv("boolean_check_message_manually_0.csv")
    }
    context['sr_output_need']['TASK_RUN']  = "CHECK"
    check_string = main.display_check_string(context)
    assert check_string == "No need to check - messages processed automatically"

def test_main(read_csv, read_yml_as_serie):
    
    # this test the main function with all inputs mocked
    mock_initiate_local_dict = {
         "df_paths": read_csv("paths.csv"),
         "sr_output_need" : read_csv("output_need_calculate.csv").iloc[0],
         "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
         'df_task_done' : read_csv("task_done.csv"),
         "df_boolean_check_message_manually" : read_csv("boolean_check_message_manually_0.csv")
    }
    mock_sr_output_need = read_csv("output_need_calculate.csv").iloc[0]
    mock_extraction_time_utc = '2025-01-01 18:00:00'

    with patch.object(main.env,"check_environment_variable"),\
         patch.object(main.dropbox,"initiate_folder"), \
         patch.object(main.local_environment_manipulation,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(main.output_need_calculation,"generate_output_need", return_value=mock_sr_output_need), \
         patch.object(main.dropbox,"download_needed_files",return_value = {}),\
         patch.object(main,"process_games",return_value = mock_initiate_local_dict), \
         patch.object(main,"process_messages",return_value = mock_initiate_local_dict), \
         patch.object(main.snowflake_etl_process,"delete_tables_data_from_python"), \
         patch.object(main.snowflake_etl_process,"update_snowflake"), \
         patch.object(main.tasks_calendar_management,"update_calendar_related_files",return_value = mock_extraction_time_utc), \
         patch.object(main.output_message_generation,"generate_output_message", return_value=({"key": "value"},MagicMock())), \
         patch.object(main.messages_posting_process,"post_message"), \
         patch.object(main.local_environment_manipulation,"terminate_local_environment"), \
         patch.object(main,"create_json_file_email"):

        main.main()
