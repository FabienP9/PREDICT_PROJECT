'''
This tests file concern all functions in the dropbox_files_interaction module.
It units test unexpected paths
'''
import os
import subprocess
from unittest.mock import MagicMock, patch

from src.predict_core.files_manipulation.external_files_interaction import dropbox_files_interaction

def test_download_file_already_exists():
    
    # this test the function download_file with a file already existing. Must accept it without downloading it again
    dropbox_file_path = "file.txt"
    local_folder = "local"

    with patch.object(os.path,"exists", return_value=True), \
         patch.object(dropbox_files_interaction.files_manipulation,"read_txt", return_value="text"):

        result = dropbox_files_interaction.download_file(dropbox_file_path, local_folder)
        assert "str_file" in result

def test_download_file_rclone_failure(assert_exit):
    
    # this test the function download_file with a failed rclone command. Must exit the program
    dropbox_file_path = "file.txt"
    local_folder = "local"

    with patch.object(os.path,"exists", return_value=False), \
         patch.object(dropbox_files_interaction.files_manipulation,"read_txt", return_value="text"), \
         patch.object(dropbox_files_interaction.var,'DROPBOX_FOLDER', 'dropbox/'), \
         patch.object(dropbox_files_interaction.subprocess,'run', return_value=subprocess.CompletedProcess(args=[], returncode=1, stderr="rclone error", stdout="")):

        assert_exit(lambda: dropbox_files_interaction.download_file(dropbox_file_path, local_folder))

def test_get_locally_dropbox_failure(read_csv, assert_exit):
    
    # this test the function get_locally forcing an error coming from dropbox. Must exit the program.
    file_name = 'game'
    local_folder = 'local_folder'
    df_paths = read_csv('paths.csv')

    with patch.object(dropbox_files_interaction,"download_file", side_effect=Exception("network error")):
        assert_exit(lambda: dropbox_files_interaction.get_locally(file_name, local_folder, df_paths))

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

        dropbox_files_interaction.copy_folder(remote_source_folder, remote_target_folder)

def test_copy_folder_fail_lsf_command(assert_exit):
    
    # this test the function copy_folder with a failed rclone command. Must exit the program
    remote_source_folder = 'source'
    remote_target_folder = 'target'

    with patch("subprocess.run") as mock_result_list:
        mock_result_list.return_value = subprocess.CompletedProcess(args=[], returncode=1, stderr="lsf failed", stdout="")
        assert_exit(lambda: dropbox_files_interaction.copy_folder(remote_source_folder, remote_target_folder))

def test_upload_file_folder_path_edge():
    
    # this test the function upload_file with a path ending with '/'. Must understand it and run
    local_file_path = "myfile.csv"
    remote_file_path = "folder/"
    
    with patch("subprocess.run") as mock_run:

        mock_run.return_value = MagicMock(returncode=0)
        dropbox_files_interaction.upload_file(local_file_path, remote_file_path)
        assert mock_run.called
    
def test_upload_file_fail(assert_exit):
    
    # this test the function upload_file with a rclone command failing. Must exit the program
    local_file_path = "myfile.csv"
    remote_file_path = "folder/myfile.csv"
    
    with patch('subprocess.run', return_value=subprocess.CompletedProcess(args=[], returncode=1, stderr="upload fail", stdout="")):

        assert_exit(lambda: dropbox_files_interaction.upload_file(local_file_path, remote_file_path))

def test_download_folder_empty_listing(read_csv):
    
    # this test the function download_folder with an empty folder. Must be accepted but without nothing getting locally
    folder_name = "database_folder"
    df_paths = read_csv("paths.csv")
    local_folder = "local_folder"

    with patch("subprocess.run", return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")), \
         patch.object(dropbox_files_interaction,"multithread_run") as mock_thread:

        dropbox_files_interaction.download_folder(folder_name, df_paths, local_folder)
        mock_thread.assert_called_with(dropbox_files_interaction.get_locally, [])
    
def test_download_folder_listing_error(read_csv,assert_exit):
    
    # this test the function download_folder with a rclone command failing for listing files. Ñust exit the program
    folder_name = "database_folder"
    df_paths = read_csv("paths.csv")
    local_folder = "local_folder"

    with patch("subprocess.run", return_value=subprocess.CompletedProcess(args=[], returncode=1, stderr="lsf error", stdout="")), \
         patch.object(dropbox_files_interaction,"multithread_run"):

        assert_exit(lambda: dropbox_files_interaction.download_folder(folder_name, df_paths, local_folder))
