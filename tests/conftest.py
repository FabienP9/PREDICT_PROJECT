import pytest
import pandas as pd
import yaml
import json
from pathlib import Path

#variable MATERIALS_DIR used in test
@pytest.fixture(scope="session")
def materials_dir():
    return Path(__file__).parent / "materials"

# for asserting it exits the program
@pytest.fixture
def assert_exit():
    def _checker(test_func, expected_code=1):
        try:
            test_func()
        except SystemExit as e: # NOSONAR
            assert e.code == expected_code, f"Expected exit code {expected_code}, got {e.code}" # NOSONAR
        else:
            raise AssertionError("SystemExit was not raised")
    return _checker

#to read txt file
@pytest.fixture
def read_txt(materials_dir):
    def _reader(filename):
        file_path = materials_dir / filename
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    return _reader

#to read yml file
@pytest.fixture
def read_yml(materials_dir):
    
    def _reader(filename):
        file_path = materials_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Test material file not found: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    return _reader

#to read json file
@pytest.fixture
def read_json(materials_dir):
    def _reader(filename):
        file_path = materials_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Test material file not found: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    return _reader

# to read_csv
@pytest.fixture
def read_csv(materials_dir):
    
    def _reader(filename, **kwargs):
        file_path = materials_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Test material file not found: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            return pd.read_csv(file, **kwargs)
    return _reader

# to write yml
@pytest.fixture
def write_yml(tmp_path):
    def _writer(filename, content):
        file_path = tmp_path / filename
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        return file_path
    return _writer