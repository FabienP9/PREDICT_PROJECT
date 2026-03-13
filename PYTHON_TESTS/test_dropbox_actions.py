'''
This tests file concern all functions in the dropbox_actions module.
It units test the happy path for each function
'''
import pandas as pd
import unittest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import dropbox_actions

def test_copy_folder():
    
    # this test the function copy folder
    remote_source_folder = 'source'
    remote_target_folder = 'target'

    with patch("subprocess.run") as mock_result_list:

        #we return file1 and file2 as list
        mock_result_list.side_effect = [
            MagicMock(returncode=0, stdout="file1\nfile2"),  
            MagicMock(returncode=0)
        ]

        dropbox_actions.copy_folder(remote_source_folder, remote_target_folder)

        assert mock_result_list.call_count == 2
        args_lsf = mock_result_list.call_args_list[0][0][0]
        assert "lsf" in args_lsf

def test_initiate_folder():
    
    # this test the function initiate_folder
    with patch.object(dropbox_actions,"copy_folder") as mock_copy:
        dropbox_actions.initiate_folder()

        mock_copy.assert_any_call("current", "-1")
        mock_copy.assert_any_call("global_manual_inputs", "current/inputs/manual", sourcepath_from_root=1, targetpath_from_root=0, sync_folder=0)
        mock_copy.assert_any_call("local_manual_inputs", "current/inputs/manual",sync_folder=0)

def test_download_file():
    
    # this test the function download_file
    dropbox_file_path = "file.txt"
    local_folder = "local"

    with patch.object(os.path,"exists", return_value=True), \
         patch.object(dropbox_actions.fileA,"read_txt", return_value="text"):

        result = dropbox_actions.download_file(dropbox_file_path, local_folder)
        assert "str_file" in result

def test_upload_file():
   
    # this test the function upload_file
    local_file_path = "myfile.csv"
    remote_file_path = "folder/myfile.csv"
    
    with patch("subprocess.run") as mock_run:

        mock_run.return_value = MagicMock(returncode=0)
        dropbox_actions.upload_file(local_file_path, remote_file_path)
        assert mock_run.called

def test_download_folder():
    
    folder_name = "database_folder"
    df_paths = pd.read_csv("materials/paths.csv")
    local_folder = "local_folder"

    # this test the function download_folder
    with patch("subprocess.run") as mock_run, \
         patch.object(dropbox_actions.config,"multithreading_run") as mock_thread:

        #folder1 having file1 and file2 inside listed
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="file1.csv\nfile2.txt")
        ]

        dropbox_actions.download_folder(folder_name, df_paths, local_folder)
        mock_thread.assert_called_once()

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_copy_folder))
    test_suite.addTest(unittest.FunctionTestCase(test_initiate_folder))
    test_suite.addTest(unittest.FunctionTestCase(test_download_file))
    test_suite.addTest(unittest.FunctionTestCase(test_upload_file))
    test_suite.addTest(unittest.FunctionTestCase(test_download_folder))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
