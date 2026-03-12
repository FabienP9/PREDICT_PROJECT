'''
This tests file concern all functions in the dropbox_files_interaction module.
It units test the happy path for each function
'''
import os
from unittest.mock import MagicMock, patch

from src.predict_core.files_manipulation.external_files_interaction import dropbox_files_interaction

def test_download_file():
    
    # this test the function download_file
    dropbox_file_path = "file.txt"
    local_folder = "local"

    with patch.object(os.path,"exists", return_value=True), \
         patch.object(dropbox_files_interaction.files_manipulation,"read_txt", return_value="text"):

        result = dropbox_files_interaction.download_file(dropbox_file_path, local_folder)
        assert "str_file" in result

def test_get_locally(read_csv):
    
    # this test the function get_locally
    file_name = 'game'
    local_folder = 'local_folder'
    df_paths = read_csv("paths.csv")
    mock_df_game = read_csv("game.csv")
    
    with patch.object(dropbox_files_interaction,"download_file", return_value={"df_game": mock_df_game}):
        result = dropbox_files_interaction.get_locally(file_name, local_folder, df_paths)
        assert "df_game" in result

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

        dropbox_files_interaction.copy_folder(remote_source_folder, remote_target_folder)

        assert mock_result_list.call_count == 2
        args_lsf = mock_result_list.call_args_list[0][0][0]
        assert "lsf" in args_lsf

def test_initiate_folder():
    
    # this test the function initiate_folder
    with patch.object(dropbox_files_interaction,"copy_folder") as mock_copy:
        dropbox_files_interaction.initiate_folder()

        mock_copy.assert_any_call("current", "-1")
        mock_copy.assert_any_call("global_manual_inputs", "current/inputs/manual", sourcepath_from_root=1, targetpath_from_root=0, sync_folder=0)
        mock_copy.assert_any_call("local_manual_inputs", "current/inputs/manual",sync_folder=0)

def test_upload_file():
   
    # this test the function upload_file
    local_file_path = "myfile.csv"
    remote_file_path = "folder/myfile.csv"
    
    with patch("subprocess.run") as mock_run:

        mock_run.return_value = MagicMock(returncode=0)
        dropbox_files_interaction.upload_file(local_file_path, remote_file_path)
        assert mock_run.called

def test_download_folder(read_csv):
    
    # this test the function download_folder
    folder_name = "database_folder"
    df_paths = read_csv("paths.csv")
    local_folder = "local_folder"

    with patch("subprocess.run") as mock_run, \
         patch.object(dropbox_files_interaction,"multithread_run") as mock_thread:

        #folder1 having file1 and file2 inside listed
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="file1.csv\nfile2.txt")
        ]

        dropbox_files_interaction.download_folder(folder_name, df_paths, local_folder)
        mock_thread.assert_called_once()

def test_download_needed_files(read_csv):
    
    # this test the download_needed files function.
    df_paths = read_csv("paths.csv")
    sr_output_need = read_csv("output_need_calculate.csv").iloc[0]

    with patch.object(dropbox_files_interaction,"multithread_run"):
        dropbox_files_interaction.download_needed_files(df_paths, sr_output_need)
