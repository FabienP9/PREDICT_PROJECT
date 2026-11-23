'''
This tests file concern all functions in the specific_files_operations module.
It units test edgecases cases
'''

import os
from unittest.mock import patch

from src.predict_core.files_manipulation.local_files_manipulation.specific_files_operations import specific_files_operations

def test_get_paths_file_details_invalid_literal(read_csv, assert_exit):
    
    # this test the function get_paths_file_details with a badly written FILTERING_COLUMN. Must exit the program
    mock_data_dict = {"df_paths": read_csv("edgecases/paths_with_bad_filtering_column.csv")}

    with patch.object(specific_files_operations.dropbox,"download_file", return_value=mock_data_dict):
        assert_exit(lambda: specific_files_operations.get_paths_file_details(), expected_code = 1) # hard exit

def test_modify_run_type_file_invalid_event(read_csv, assert_exit):
    
    # this test the function modify_run_type_file with an unexpected event. Must exit the program.
        df_run_type = read_csv("RUN_TYPE_before_initiate.csv")
        called_by = "main"
        event="unexpected"
        os.environ["IS_OUTPUT_AUTO"] = "1"

        assert_exit(lambda: specific_files_operations.modify_run_type_file(df_run_type,called_by,event), expected_code = 1) # hard exit