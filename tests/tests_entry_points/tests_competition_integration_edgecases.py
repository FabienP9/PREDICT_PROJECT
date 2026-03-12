'''
This tests file concern all functions in the competition_integration module.
It units test unhappy paths
'''

from unittest.mock import patch

from src.predict_core.entry_point import competition_integration

def test_dropbox_initiate_folder_failure(assert_exit):
    
    # this test the function competition_integration with a failing initiate_folder. Must exit program
    with patch.object(competition_integration.env,"check_environment_variable"),\
         patch.object(competition_integration.dropbox,"initiate_folder", side_effect=Exception("Dropbox error")):
        assert_exit(lambda: competition_integration.competition_integration())

def test_initiate_local_environment_missing_key(assert_exit):
    
    # this test the function competition_integration with a failing initiate_local_environment, because of missing keys. Must exit the program
    mock_initiate_local_dict = {}
    with patch.object(competition_integration.env,"check_environment_variable"),\
         patch.object(competition_integration.dropbox,"initiate_folder"), \
         patch.object(competition_integration.local_environment_manipulation,"initiate_local_environment", return_value = mock_initiate_local_dict):

        assert_exit(lambda: competition_integration.competition_integration())

def test_filter_data_failure(read_csv, assert_exit):
    
    # this test the function competition_integration with a failing filter_data. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": read_csv( "snowflake_account_connect.csv").iloc[0],
        "df_competition": read_csv( "competition.csv"),
        "df_paths": read_csv( "paths.csv"),
        "df_task_done": read_csv( "task_done.csv")
    }
    mock_df_game = read_csv( "game.csv")
    
    with patch.object(competition_integration.env,"check_environment_variable"),\
         patch.object(competition_integration.dropbox,"initiate_folder"), \
         patch.object(competition_integration.local_environment_manipulation,"initiate_local_environment", mock_initiate_local_dict), \
         patch.object(competition_integration.games_details_extraction,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(competition_integration.files_manipulation,"filter_data", side_effect=Exception("Filter failed")):

        assert_exit(lambda: competition_integration.competition_integration())

def test_snowflake_update_failure(read_csv, assert_exit):
    
    # this test the function competition_integration with a failing update_snowflake. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": read_csv( "snowflake_account_connect.csv").iloc[0],
        "df_competition": read_csv( "competition.csv"),
        "df_paths": read_csv( "paths.csv"),
        "df_task_done": read_csv( "task_done.csv")
    }
    mock_df_game = read_csv( "game.csv")

    with patch.object(competition_integration.env,"check_environment_variable"),\
         patch.object(competition_integration.dropbox,"initiate_folder"), \
         patch.object(competition_integration.local_environment_manipulation,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(competition_integration.games_details_extraction,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(competition_integration.files_manipulation,"filter_data"), \
         patch.object(competition_integration.snowflake_etl_process,"delete_tables_data_from_python"), \
         patch.object(competition_integration.snowflake_etl_process,"update_snowflake"):

        assert_exit(lambda: competition_integration.competition_integration())

def test_calendar_update_failure(read_csv, assert_exit):
    
    # this test the function competition_integration with a failing update_calendar_related_files. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": read_csv( "snowflake_account_connect.csv").iloc[0],
        "df_competition": read_csv( "competition.csv"),
        "df_paths": read_csv( "paths.csv"),
        "df_task_done": read_csv( "task_done.csv")
    }
    mock_df_game = read_csv( "game.csv")

    with patch.object(competition_integration.env,"check_environment_variable"),\
         patch.object(competition_integration.dropbox,"initiate_folder"), \
         patch.object(competition_integration.local_environment_manipulation,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(competition_integration.games_details_extraction,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(competition_integration.files_manipulation,"filter_data"), \
         patch.object(competition_integration.snowflake_etl_process,"delete_tables_data_from_python"), \
         patch.object(competition_integration.snowflake_etl_process,"update_snowflake"), \
         patch.object(competition_integration,"update_calendar_related_files", side_effect=Exception("Calendar error")):
    
        assert_exit(lambda: competition_integration.competition_integration())

def test_terminate_local_environment_failure(read_csv, assert_exit):
    
    # this test the function competition_integration with a failing terminate_local_environment. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect": read_csv( "snowflake_account_connect.csv").iloc[0],
        "df_competition": read_csv( "competition.csv"),
        "df_paths": read_csv( "paths.csv"),
        "df_task_done": read_csv( "task_done.csv")
    }
    mock_df_game = read_csv( "game.csv")
    mock_str_next_run_time_utc = "2024-01-02 08:05:00.000"

    with patch.object(competition_integration.env,"check_environment_variable"),\
         patch.object(competition_integration.dropbox,"initiate_folder"), \
         patch.object(competition_integration.local_environment_manipulation,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(competition_integration.games_details_extraction,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(competition_integration.files_manipulation,"filter_data"), \
         patch.object(competition_integration.snowflake_etl_process,"delete_tables_data_from_python"), \
         patch.object(competition_integration.snowflake_etl_process,"update_snowflake"), \
         patch.object(competition_integration,"update_calendar_related_files", return_value=mock_str_next_run_time_utc), \
         patch.object(competition_integration.local_environment_manipulation,"terminate_local_environment", side_effect=Exception("Terminate failed")):

        assert_exit(lambda: competition_integration.competition_integration())