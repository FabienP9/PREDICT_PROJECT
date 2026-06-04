'''
This tests file concern all functions in the specific_files_operations module.
It units test edgecases cases
'''

import os
from unittest.mock import patch
import json

from src.predict_core.files_manipulation.local_files_manipulation.specific_files_operations import specific_files_operations

def test_get_paths_file_details_invalid_literal(read_csv, assert_exit):
    
    # this test the function get_paths_file_details with a badly written FILTERING_COLUMN. Must exit the program
    mock_data_dict = {"df_paths": read_csv("edgecases/paths_with_bad_filtering_column.csv")}

    with patch.object(specific_files_operations.dropbox,"download_file", return_value=mock_data_dict):
        assert_exit(lambda: specific_files_operations.get_paths_file_details())

def test_modify_run_type_file_invalid_event(read_csv, assert_exit):
    
    # this test the function modify_run_type_file with an unexpected event. Must exit the program.
        df_run_type = read_csv("RUN_TYPE_before_initiate.csv")
        called_by = "main"
        event="unexpected"
        os.environ["IS_OUTPUT_AUTO"] = "1"

        assert_exit(lambda: specific_files_operations.modify_run_type_file(df_run_type,called_by,event))

def test_personalize_yml_missing_env_vars(assert_exit):
    
    # this test the function personalize_yml_dbt_file with the environment variables non provided. Must exit the program.
    with patch.object(specific_files_operations.files_manipulation,"read_yml_as_txt", return_value="#DATABASE# something"), \
         patch.dict("os.environ", {}, clear=True), \
         patch.object(specific_files_operations.files_manipulation,"create_yml"):
        
        sr = {"ACCOUNT": "acc", "DATABASE_PROD": "prod", "DATABASE_TEST": "test", "WAREHOUSE": "wh"}
        assert_exit(lambda: specific_files_operations.personalize_yml_dbt_file("profiles.yml", sr))

def test_parametrize_yml_dbt_file_bad_read(assert_exit):
    
    # this test the function parametrize_yml_dbt_file forcing an error reading it. Must exit the program.
    with patch.object(specific_files_operations.files_manipulation,"read_yml_as_txt", side_effect=OSError("cannot read")):
        assert_exit(lambda: specific_files_operations.parametrize_yml_dbt_file("profiles.yml"))

def test_create_json_file_email_empty():

    # this test the function create_json_file with empty args. Must accept it.
    filename = "json_file_email_details.json"
    expected_data = {}
    specific_files_operations.create_json_file_email()
    with open(filename, "r") as f:
        data = json.load(f)
    
    assert data == expected_data
    os.remove(filename)
