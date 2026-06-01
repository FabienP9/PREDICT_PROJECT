'''
This tests file concern all functions in the local_environment_manipulation module.
It units test edgecases cases
'''

import os
from unittest.mock import patch

from src.predict_core.files_manipulation.local_files_manipulation import local_environment_manipulation
import src.predict_core.config.config_variables.config_global_variables as var
from src.predict_core.config import config_decorators

def test_create_local_folder_when_already_exists():
    
    # this test the function create_local_folder with folder already existing, without errors
    local_environment_manipulation.create_local_folder()
    assert os.path.exists(var.TMPF)
    assert os.path.exists(var.TMPD)
    with patch.object(config_decorators,"sys_exit") as mock_exit:
        local_environment_manipulation.create_local_folder()
        mock_exit.assert_not_called()
        assert os.path.exists(var.TMPF)
        assert os.path.exists(var.TMPD)

def test_initiate_local_environment_missing_flag(read_yml_as_serie, read_csv, assert_exit):
    
    # this test initial_local_environment with unknown caller. Must exit the program
    called_by = "unknown caller"
    mock_df_paths_dict = {
        "df_paths": read_csv("paths.csv")
    }
    mock_sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    mock_str_next_run_time_utc = "2024-01-01 08:01:00.000"
    mock_df_run_type = read_csv("RUN_TYPE_after_initiate.csv")
    mock_data_dict = {
        "sr_snowflake_account_connect": mock_sr_snowflake_account_connect,
        "str_next_run_time_utc": mock_str_next_run_time_utc,
        "df_run_type": mock_df_run_type,
    }

    with patch.object(local_environment_manipulation,"create_local_folder"), \
         patch.object(local_environment_manipulation.specific_files_operations,"get_paths_file_details", return_value=mock_df_paths_dict), \
         patch.object(local_environment_manipulation,"multithread_run", return_value=[mock_data_dict]), \
         patch.object(local_environment_manipulation.specific_files_operations,"modify_run_type_file", return_value=mock_df_run_type), \
         patch.object(local_environment_manipulation.dropbox,"upload_file"), \
         patch.object(local_environment_manipulation.specific_files_operations,"personalize_yml_dbt_file"):
        
        assert_exit(lambda: local_environment_manipulation.initiate_local_environment(called_by))

def test_initiate_local_environment_empty_df_paths(read_yml_as_serie, read_csv, assert_exit):
    
    # this test initial_local_environment with an empty df_paths. Must exit the program
    called_by = "main"
    mock_df_paths_dict = {
        "df_paths": read_csv("edgecases/paths_empty.csv")
    }
    mock_sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    mock_str_next_run_time_utc = "2024-01-01 08:01:00.000"
    mock_df_run_type = read_csv("RUN_TYPE_after_initiate.csv")
    mock_data_dict = {
        "sr_snowflake_account_connect": mock_sr_snowflake_account_connect,
        "str_next_run_time_utc": mock_str_next_run_time_utc,
        "df_run_type": mock_df_run_type,
    }

    with patch.object(local_environment_manipulation,"create_local_folder"), \
         patch.object(local_environment_manipulation.specific_files_operations,"get_paths_file_details", return_value=mock_df_paths_dict), \
         patch.object(local_environment_manipulation,"multithread_run", return_value=[mock_data_dict]), \
         patch.object(local_environment_manipulation.specific_files_operations,"modify_run_type_file", return_value=mock_df_run_type), \
         patch.object(local_environment_manipulation.dropbox,"upload_file"), \
         patch.object(local_environment_manipulation.specific_files_operations,"personalize_yml_dbt_file"):
        
        assert_exit(lambda: local_environment_manipulation.initiate_local_environment(called_by))
