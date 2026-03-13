'''
This tests file concern all functions in the dropbox_actions module.
It units test unexpected paths for each function
'''
import unittest
from unittest.mock import patch, MagicMock
import subprocess
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import dropbox_actions
from testutils import assertExit

def test_copy_folder_empty_source_or_target():
    
    # this test the function copy_folder with empty sources and target. Must be accepted
    remote_source_folder = ''
    remote_target_folder = ''

    with patch("subprocess.run") as mock_result_list:

        #we return file1 and file2 as list
        mock_result_list.side_effect = [
            MagicMock(returncode=0, stdout="file1\nfile2"),  
            MagicMock(returncode=0)
        ]

        dropbox_actions.copy_folder(remote_source_folder, remote_target_folder)

def test_copy_folder_fail_lsf_command():
    
    # this test the function copy_folder with a failed rclone command. Must exit the program
    remote_source_folder = 'source'
    remote_target_folder = 'target'

    with patch("subprocess.run") as mock_result_list:
        mock_result_list.return_value = subprocess.CompletedProcess(args=[], returncode=1, stderr="lsf failed", stdout="")
        assertExit(lambda: dropbox_actions.copy_folder(remote_source_folder, remote_target_folder))

def test_download_file_already_exists():
    
    # this test the function download_file with a file already existing. Must accept it without downloading it again
    dropbox_file_path = "file.txt"
    local_folder = "local"

    with patch.object(os.path,"exists", return_value=True), \
         patch.object(dropbox_actions.fileA,"read_txt", return_value="text"):

        result = dropbox_actions.download_file(dropbox_file_path, local_folder)
        assert "str_file" in result

def test_download_file_rclone_failure():
    
    # this test the function download_file with a failed rclone command. Must exit the program
    dropbox_file_path = "file.txt"
    local_folder = "local"

    with patch.object(os.path,"exists", return_value=False), \
         patch.object(dropbox_actions.fileA,"read_txt", return_value="text"), \
         patch.object(dropbox_actions.config,'dropbox_folder', 'dropbox/'), \
         patch.object(dropbox_actions.subprocess,'run', return_value=subprocess.CompletedProcess(args=[], returncode=1, stderr="rclone error", stdout="")):

        assertExit(lambda: dropbox_actions.download_file(dropbox_file_path, local_folder))
       
def test_upload_file_folder_path_edge():
    
    # this test the function upload_file with a path ending with '/'. Must understand it and run
    local_file_path = "myfile.csv"
    remote_file_path = "folder/"
    
    with patch("subprocess.run") as mock_run:

        mock_run.return_value = MagicMock(returncode=0)
        dropbox_actions.upload_file(local_file_path, remote_file_path)
        assert mock_run.called
    
def test_upload_file_fail():
    
    # this test the function upload_file with a rclone command failing. Must exit the program
    local_file_path = "myfile.csv"
    remote_file_path = "folder/myfile.csv"
    
    with patch('dropbox_actions.subprocess.run', return_value=subprocess.CompletedProcess(args=[], returncode=1, stderr="upload fail", stdout="")):

        assertExit(lambda: dropbox_actions.upload_file(local_file_path, remote_file_path))
        
def test_download_folder_empty_listing():
    
    # this test the function download_folder with an empty folder. Must be accepted but without nothing getting locally
    folder_name = "database_folder"
    df_paths = pd.read_csv("materials/paths.csv")
    local_folder = "local_folder"

    with patch("subprocess.run", return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")), \
         patch.object(dropbox_actions.config,"multithreading_run") as mock_thread:

        dropbox_actions.download_folder(folder_name, df_paths, local_folder)
        mock_thread.assert_called_with(dropbox_actions.fileA.get_locally_from_dropbox, [])
    
def test_download_folder_listing_error():
    
    # this test the function download_folder with a rclone command failing for listing files. Ñust exit the program
    folder_name = "database_folder"
    df_paths = pd.read_csv("materials/paths.csv")
    local_folder = "local_folder"

    with patch("subprocess.run", return_value=subprocess.CompletedProcess(args=[], returncode=1, stderr="lsf error", stdout="")), \
         patch.object(dropbox_actions.config,"multithreading_run"):

        assertExit(lambda: dropbox_actions.download_folder(folder_name, df_paths, local_folder))
        
if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_copy_folder_empty_source_or_target))
    test_suite.addTest(unittest.FunctionTestCase(test_copy_folder_fail_lsf_command))
    test_suite.addTest(unittest.FunctionTestCase(test_download_file_already_exists))
    test_suite.addTest(unittest.FunctionTestCase(test_download_file_rclone_failure))
    test_suite.addTest(unittest.FunctionTestCase(test_upload_file_folder_path_edge))
    test_suite.addTest(unittest.FunctionTestCase(test_upload_file_fail))
    test_suite.addTest(unittest.FunctionTestCase(test_download_folder_empty_listing))
    test_suite.addTest(unittest.FunctionTestCase(test_download_folder_listing_error))
    
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
