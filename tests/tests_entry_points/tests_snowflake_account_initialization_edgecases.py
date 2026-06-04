'''
This tests file concern all functions in the snowflake_account_initialization module.
It units test unhappy paths
'''
from unittest.mock import patch

from src.predict_core.entry_point import snowflake_account_initialization

def test_dropbox_init_failure(assert_exit):
    
    # this test the function snowflake_account_initialization with a failing initiate_folder. Must exit the program
    with patch.object(snowflake_account_initialization.env,"check_environment_variable"),\
         patch.object(snowflake_account_initialization.dropbox,"initiate_folder", side_effect=Exception("Dropbox init failed")):
            
            assert_exit(lambda: snowflake_account_initialization.snowflake_account_initialization())

def test_local_env_failure(assert_exit):
    
    # this test the function snowflake_execute_script with a failing initiate_local_environment. Must exit the program
    with patch.object(snowflake_account_initialization.env,"check_environment_variable"),\
         patch.object(snowflake_account_initialization.dropbox,"initiate_folder"), \
         patch.object(snowflake_account_initialization.local_environment_manipulation,"initiate_local_environment", side_effect=Exception("Local env failed")):

          assert_exit(lambda: snowflake_account_initialization.snowflake_account_initialization())

def test_snowflake_script_failure(read_csv, read_yml_as_serie, assert_exit):
    
    # this test the function snowflake_account_initialization with a failing snowflake_execute_script. Must exit the program
    mock_initiate_local_dict = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        "str_script_creating_database" : "",
        "df_paths": read_csv("paths.csv")
    }

    with patch.object(snowflake_account_initialization.env,"check_environment_variable"),\
         patch.object(snowflake_account_initialization.dropbox,"initiate_folder"), \
         patch.object(snowflake_account_initialization.local_environment_manipulation,"initiate_local_environment", return_value=mock_initiate_local_dict), \
         patch.object(snowflake_account_initialization.snowflake_connection_execution,"snowflake_execute_script", side_effect=Exception("Snowflake script failed")):

          assert_exit(lambda: snowflake_account_initialization.snowflake_account_initialization())

def test_dropbox_download_failure(read_yml_as_serie, read_csv, assert_exit):
    
    # this test the function snowflake_account_initialization with a failing download_folder. Must exit the program
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
         patch.object(snowflake_account_initialization.dropbox,"download_folder", side_effect=Exception("Download failed")):

          assert_exit(lambda: snowflake_account_initialization.snowflake_account_initialization())

def test_update_snowflake_failure(read_yml_as_serie, read_csv, assert_exit):
    
    # this test the function snowflake_account_initialization with a failing update_snowflake. Must exit the program
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
         patch.object(snowflake_account_initialization.snowflake_etl_process,"update_snowflake", side_effect=Exception("Update failed")):

          assert_exit(lambda: snowflake_account_initialization.snowflake_account_initialization())

def test_terminate_local_env_failure(read_csv, read_yml_as_serie, assert_exit):
    
    # this test the function snowflake_account_initialization with a failing terminate_local_environment. Must exit the program
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
         patch.object(snowflake_account_initialization.local_environment_manipulation,"terminate_local_environment", side_effect=Exception("Terminate failed")):

          assert_exit(lambda: snowflake_account_initialization.snowflake_account_initialization())