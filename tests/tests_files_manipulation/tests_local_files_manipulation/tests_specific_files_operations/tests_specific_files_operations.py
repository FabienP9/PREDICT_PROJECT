'''
This tests file concern all functions in the specific_files_operations module.
It units test the happy path for each function
'''

import os
from unittest.mock import patch
from pandas.testing import assert_frame_equal
import pandas as pd
import json

from src.predict_core.files_manipulation.local_files_manipulation.specific_files_operations import specific_files_operations
from src.predict_core.config.config_variables import config_global_variables as var
def test_get_paths_file_details(read_csv):
    
    # this test the function get_paths_file_details
    mock_data_dict = {"df_paths": read_csv("paths.csv")}

    with patch.object(specific_files_operations.dropbox,"download_file", return_value=mock_data_dict):
        result = specific_files_operations.get_paths_file_details()

        assert isinstance(result, dict)
        assert "df_paths" in result
        df = result["df_paths"]
        # Ensure columns are converted to lists
        assert all(isinstance(df.loc[0, c], list) for c in ["FILTERING_COLUMN", "DOWNLOAD_CATEGORY", "PYTHON_CATEGORY", "DBT_CATEGORY"])

def test_modify_run_type_file(read_csv):
    
    # this test the function modify_run_type_file
    df_run_type = read_csv("RUN_TYPE_before_initiate.csv")
    called_by = "main"
    event="initiate"
    os.environ["IS_OUTPUT_AUTO"] = "1"
    expected_df = read_csv("RUN_TYPE_after_initiate.csv") #we won't check RUN_TIME_UTC
    with patch.object(specific_files_operations.files_manipulation,"create_csv"):
        result_df = specific_files_operations.modify_run_type_file(df_run_type,called_by,event)
        assert_frame_equal(result_df[1:].astype(str).reset_index(drop=True), expected_df[1:].astype(str).reset_index(drop=True),check_dtype=False)
        assert result_df.at[0,'EVENT'] == expected_df.at[0,'EVENT'] 
        assert result_df.at[0,'RUN_TYPE'] == expected_df.at[0,'RUN_TYPE'] 
        assert (result_df.at[0,'RUN_METHOD'] == expected_df.at[0,'RUN_METHOD']) or (pd.isna(result_df.at[0,'RUN_METHOD']) and pd.isna(expected_df.at[0,'RUN_METHOD']))
        assert int(result_df.at[0,'OUTPUT_AUTO']) == int(expected_df.at[0,'OUTPUT_AUTO']) 
        assert (result_df.at[0,'PLANNED_RUN_TIME_UTC'] == expected_df.at[0,'PLANNED_RUN_TIME_UTC']) or (pd.isna(result_df.at[0,'PLANNED_RUN_TIME_UTC']) and pd.isna(expected_df.at[0,'PLANNED_RUN_TIME_UTC']))

def test_create_json_file_email():

    # this test the function create_json_file with empty args. Must accept it.
    filename = "json_file_email_details.json"
    var.JSON_EMAIL_FILE_DICT = {
        "str1": "Hello",
        "str2": "goodbye"
    }
    expected_data = {
        "str1": "Hello",
        "str2": "goodbye"
    }
    specific_files_operations.create_json_file_email()
    with open(filename, "r") as f:
        data = json.load(f)
    
    assert data == expected_data
    os.remove(filename)