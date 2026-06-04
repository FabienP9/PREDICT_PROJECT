'''
This tests file concern all functions in the competition_integration module.
It units test the happy path for each function
'''

from unittest.mock import patch

from src.predict_core.entry_point import competition_integration

def test_competition_integration(read_yml_as_serie, read_csv):
    
    # this test the function competition_integration mocking all dependencies
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        "df_competition": read_csv("competition.csv"),
        "df_paths": read_csv("paths.csv"),
        "df_task_done": read_csv("task_done.csv")
    }
    mock_df_game = read_csv("game.csv")
    mock_str_next_run_time_utc = "2024-01-02 08:05:00.000"

    with patch.object(competition_integration.env,"check_environment_variable"),\
         patch.object(competition_integration.dropbox,"initiate_folder"), \
         patch.object(competition_integration.local_environment_manipulation,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(competition_integration.games_details_extraction,"extract_games_from_competition", return_value=mock_df_game), \
         patch.object(competition_integration.files_manipulation,"filter_data"), \
         patch.object(competition_integration.snowflake_etl_process,"delete_tables_data_from_python"), \
         patch.object(competition_integration.snowflake_etl_process,"update_snowflake"), \
         patch.object(competition_integration,"update_calendar_related_files", return_value=mock_str_next_run_time_utc), \
         patch.object(competition_integration.local_environment_manipulation,"terminate_local_environment"), \
         patch.object(competition_integration,"create_json_file_email"):

        competition_integration.competition_integration()

