'''
This tests file concern all functions in the snowflake_account_initialization module.
It units test the happy path for each function
'''
from unittest.mock import patch

from src.predict_core.entry_point import snowflake_account_initialization

def test_snowflake_account_initialization_happy_path(read_yml_as_serie, read_csv):
    
    # this test the function snowflake_account_initialization mocking all dependencies
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        "str_script_creating_database" : "", \
        "df_paths": read_csv("paths.csv"),

        "df_competition": read_csv("competition.csv"),
        "df_task_done": read_csv("task_done.csv")
    }

    with patch.object(snowflake_account_initialization.env,"check_environment_variable"),\
         patch.object(snowflake_account_initialization.dropbox,"initiate_folder"), \
         patch.object(snowflake_account_initialization.local_environment_manipulation,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(snowflake_account_initialization.snowflake_connection_execution,"snowflake_execute_script"), \
         patch.object(snowflake_account_initialization.dropbox,"download_folder"), \
         patch.object(snowflake_account_initialization.snowflake_etl_process,"update_snowflake"), \
         patch.object(snowflake_account_initialization.local_environment_manipulation,"terminate_local_environment"):

          snowflake_account_initialization.snowflake_account_initialization()
