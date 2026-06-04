'''
This tests file concern all functions in the files_manipulation module.
It units test the happy path for each function
'''

import os
import tempfile
from unittest.mock import patch, MagicMock, mock_open
from pandas.testing import assert_frame_equal
import matplotlib.pyplot as plt
import pandas as pd

from src.predict_core.files_manipulation.local_files_manipulation import files_manipulation

def test_create_csv(read_csv):
    
    # this test the function create_csv
    local_file_path = "create_csv.csv"
    df = read_csv("read_csv.csv")

    m = MagicMock()
    with patch.object(df, "to_csv", m):
        files_manipulation.create_csv(local_file_path, df)
        assert m.called

def test_create_txt(read_txt):
    
    # this test the function create_txt
    local_file_path = "create_txt.txt"
    s = read_txt("read_txt.txt")

    m = mock_open()
    with patch("builtins.open", m):
        files_manipulation.create_txt(local_file_path, s)
        m().write.assert_called_once_with("hello world!")

def test_create_jpg():
    
    # this test the function test_create_jpg creating a temp folder
    with tempfile.TemporaryDirectory() as tmpdir:
        
        local_file_path = tmpdir+"/create_jpg.jpg"
        fig, ax = plt.subplots()
        ax.plot([1, 2, 3], [4, 5, 6])
        ax.set_title("Happy Path Test")
        
        files_manipulation.create_jpg(local_file_path, fig)
        assert os.path.exists(local_file_path), "Expected JPG file was not created."
        assert os.path.getsize(local_file_path) > 0, "Created JPG file is empty."

        plt.close(fig)

def test_filter_data():
    
    # this test the function filter_data with two dependant files
    data_dict = {
        "df_df1": pd.DataFrame({"col": [1, 2]}),
        "df_df2": pd.DataFrame({"col": [2, 3]})
    }


    df_paths = pd.DataFrame({
        "NAME": ["df1", "df2"],
        "FILTERING_CATEGORY": ["cat", "cat"],
        "FILTERING_FILE": ["", "df1"],
        "FILTERING_COLUMN": ["col", "col"],
        "IS_FOR_UPLOAD": [0, 0]
    })
    
    expected = {
        "df_df1": pd.DataFrame({"col": [1, 2]}),
        "df_df2": pd.DataFrame({"col": [2]})
    }
    with patch.object(files_manipulation,"create_csv"):
        result = files_manipulation.filter_data(data_dict, df_paths, "cat")
    
    assert_frame_equal(result["df_df1"].reset_index(drop=True), expected["df_df1"].reset_index(drop=True))
    assert_frame_equal(result["df_df2"].reset_index(drop=True), expected["df_df2"].reset_index(drop=True))
