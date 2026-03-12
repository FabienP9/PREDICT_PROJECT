'''
This tests file concern all functions in the files_manipulation module.
It units test edgecases cases
'''
import tempfile
from unittest.mock import patch, mock_open, MagicMock

from src.predict_core.files_manipulation.local_files_manipulation import files_manipulation

def test_read_json_file_not_found(assert_exit):
    
    # this test the function read_json with a file non existant. Must exit the program
    with patch("builtins.open", side_effect=FileNotFoundError("no file")):
        assert_exit(lambda: files_manipulation.read_json("does_not_exist.json"))

def test_read_json_invalid_json(assert_exit):
    
    # this test the function read_json with a file json type invalid. Must exit the program
    with patch("builtins.open", mock_open(read_data="not-json")), \
         patch("json.load", side_effect=ValueError("bad json")):
        assert_exit(lambda: files_manipulation.read_json("bad.json"))

def test_read_and_check_csv_missing_columns(read_json, assert_exit):
    
    # this test the function read_and_check_csv with a file having a missing column. Must exit the program
    local_file_path = "read_csv.csv"
    mock_schema = read_json("edgecases/read_csv_schema_with_fake_column.json")

    with patch.object(files_manipulation, "read_json", return_value=mock_schema):
        assert_exit(lambda: files_manipulation.read_and_check_csv(local_file_path))

def test_read_and_check_csv_type_mismatch(read_csv, read_json, assert_exit):
    
    # this test the function read_and_check_csv with a file having a column of a different type than expected. Must exit the program
    local_file_path = read_csv("edgecases/read_csv_type_mismatch.csv")
    mock_schema = read_json("read_csv_schema.json")

    with patch.object(files_manipulation, "read_json", return_value=mock_schema):
        assert_exit(lambda: files_manipulation.read_and_check_csv(local_file_path))

def test_read_yml_file_not_found(assert_exit):
    
    # this test the function read_yml with a file non existant. Must exit the program
    with patch("builtins.open", side_effect=FileNotFoundError("no file")):
        assert_exit(lambda: files_manipulation.read_yml("missing.yml"))

def test_read_txt_file_not_found(assert_exit):
    
    # this test the function read_txt with a file non existant. Must exit the program
    with patch("builtins.open", side_effect=FileNotFoundError("no file")):
        assert_exit(lambda: files_manipulation.read_txt("missing.txt"))

def test_create_csv_write_failure(read_csv, assert_exit):

    # this test the function create_csv forcing a write failure. Must exit the program.
    local_file_path = "create_csv.csv"
    df = read_csv("read_csv.csv")

    with patch("pandas.DataFrame.to_csv", side_effect=OSError("disk full")):
        assert_exit(lambda: files_manipulation.create_csv(local_file_path, df))

def test_create_yml_failure(read_yml, assert_exit):
    
    # this test the function create_yml forcing a write failure. Must exit the program.
    local_file_path = "create_yml.yml"
    s = read_yml("read_yml.yml")

    with patch("builtins.open", side_effect=OSError("cannot write")):
        assert_exit(lambda: files_manipulation.create_yml(local_file_path, s))

def test_create_txt_failure(read_txt, assert_exit):
    
    # this test the function create_txt forcing a write failure. Must exit the program.
    local_file_path = "create_txt.txt"
    s = read_txt("read_txt.txt")

    with patch("builtins.open", side_effect=OSError("cannot write")):
        assert_exit(lambda: files_manipulation.create_yml(local_file_path, s))

def test_create_jpg_save_failure(assert_exit):
    
    # this test the function create_jpg forcing an error while saving it. Must exit the program.
    with tempfile.TemporaryDirectory() as tmpdir:
        local_file_path = tmpdir+"/create_jpg.jpg"
        fake_fig = MagicMock()
        fake_fig.savefig.side_effect = Exception("save error")
        assert_exit(lambda: files_manipulation.create_jpg(local_file_path, fake_fig))
