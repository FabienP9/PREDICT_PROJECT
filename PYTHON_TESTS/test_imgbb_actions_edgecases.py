'''
This tests file concern all functions in the imgbb_actions module.
It units test edge cases paths for each function
'''

import unittest
from unittest.mock import patch, mock_open
import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
from output_message_generation_actions import imgbb_actions as imgbb_actions
from testutils import assertExit

def test_push_capture_online_missing_key():

    # this test the push_capture_online function with missing key. Must exit the program.
    if "IMGBB_API_KEY" in os.environ:
        del os.environ["IMGBB_API_KEY"]

    image_path = "image.png" 

    with patch("builtins.open", mock_open(read_data=b"fake image data")):

        assertExit(lambda: imgbb_actions.push_capture_online(image_path))

def test_push_capture_invalid_json_response():
    
    # this test the push_capture_online function with invalid json. Must exit the program.
    image_path = "image.png" 
    mock_response = type("MockResponse", (), {"json": lambda self: {'data': {"unexpected": "structure"}}})()

    with patch("builtins.open", mock_open(read_data=b"fake image data")), \
         patch("requests.post", return_value=mock_response), \
         patch("os.getenv", return_value="fake_api_key"):
            
            assertExit(lambda: imgbb_actions.push_capture_online(image_path))

def test_response_not_json():

    # this test the push_capture_online function with no json. Must exit the program.
    image_path = "image.png" 
    
    with patch("builtins.open", mock_open(read_data=b"fake image data")), \
         patch("requests.post", side_effect = ValueError("Invalid JSON")), \
         patch("os.getenv", return_value="fake_api_key"):
            
            assertExit(lambda: imgbb_actions.push_capture_online(image_path))  
    

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_push_capture_online_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_push_capture_invalid_json_response))
    test_suite.addTest(unittest.FunctionTestCase(test_response_not_json))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)