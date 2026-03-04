'''
This tests file concern all functions in the config module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import config
from testutils import write_yml
from testutils import read_yml
from testutils import assertExit

def test_onError_final_execute_parametrizes_file():
    
    # this test function onError_final_execute
    mock_profile_file = "profiles.yml"
    mock_profile_content = "account: myacc\ndatabase: mydb\npassword: mypwd\nuser: me\n"
    write_yml(mock_profile_file,mock_profile_content)
    expected_parametrized_profile_file = read_yml("materials/dbt_profile_parametrized_sample.yml")
    with patch.object(config,"dbt_profiles_path", mock_profile_file):
        config.onError_final_execute()
        content = read_yml(mock_profile_file)
        assert content == expected_parametrized_profile_file

def test_exit_program_decorator_exits():
    
    # this test decorator exit_program, by forcing an error on a created function
    @config.exit_program()
    def error():
        raise ValueError("boom")

    assertExit(lambda: error())
    
def test_retry_function_eventually_succeeds():
    
    # this test decorator retry_function with a success state at second attempt, by calling a created function
    calls = {"n": 0}
    @config.retry_function(max_attempts=3, delay_secs=0)
    def error_til_success():
        calls["n"] += 1
        if calls["n"] < 2:
            raise ValueError("fail once")
        return "ok"

    result = error_til_success()
    assert result == "ok"
    assert calls["n"] == 2

def test_retry_function_fails_after_max_attempts():

    # this test decorator retry_function eventually failing by forcing an error on a created function
    @config.retry_function()
    def error():
        raise ValueError("boom")

    with unittest.TestCase().assertRaises(ValueError):
        error()

def test_raise_issue_to_caller():
    
    # this test decorator raise_issue_to_caller by forcing an error on a created function
    @config.raise_issue_to_caller()
    def bad():
        raise RuntimeError("problem")

    with unittest.TestCase().assertRaises(RuntimeError):
        bad()

def test_create_and_destroy_local_folder():
    
    # this test functions create_local_folder and destroy_local_folder
    config.create_local_folder()
    assert os.path.exists(config.TMPF)
    assert os.path.exists(config.TMPD)

    config.destroy_local_folder()
    assert not os.path.exists(config.TMPF)
    assert not os.path.exists(config.TMPD)

def test_multithreading_run():
    
    # this test function multithreading_run with a function a+b
    def add(a, b):
        return a + b

    args = [(1, 2), (3, 4), (5, 6)]
    results = config.multithreading_run(add, args, thread_max_workers=2)
    assert sorted(results) == [3, 7, 11]

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_onError_final_execute_parametrizes_file))
    test_suite.addTest(unittest.FunctionTestCase(test_exit_program_decorator_exits))
    test_suite.addTest(unittest.FunctionTestCase(test_retry_function_eventually_succeeds))
    test_suite.addTest(unittest.FunctionTestCase(test_retry_function_fails_after_max_attempts))
    test_suite.addTest(unittest.FunctionTestCase(test_raise_issue_to_caller))
    test_suite.addTest(unittest.FunctionTestCase(test_create_and_destroy_local_folder))
    test_suite.addTest(unittest.FunctionTestCase(test_multithreading_run))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
