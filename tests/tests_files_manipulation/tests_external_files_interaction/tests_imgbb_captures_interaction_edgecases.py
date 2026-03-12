'''
This tests file concern all functions in the imgbb_captures_interaction module.
It units test edge cases paths
'''

import os
from unittest.mock import mock_open, patch

from src.predict_core.files_manipulation.external_files_interaction import imgbb_captures_interaction

def test_push_capture_online_missing_key(assert_exit):

    # this test the push_capture_online function with missing key. Must exit the program.
    if "IMGBB_API_KEY" in os.environ:
        del os.environ["IMGBB_API_KEY"]

    image_path = "image.png" 

    with patch("builtins.open", mock_open(read_data=b"fake image data")):

        assert_exit(lambda: imgbb_captures_interaction.push_capture_online(image_path))

def test_push_capture_invalid_json_response(assert_exit):
    
    # this test the push_capture_online function with invalid json. Must exit the program.
    image_path = "image.png" 
    mock_response = type("MockResponse", (), {"json": lambda self: {'data': {"unexpected": "structure"}}})()

    with patch("builtins.open", mock_open(read_data=b"fake image data")), \
         patch("requests.post", return_value=mock_response), \
         patch("os.getenv", return_value="fake_api_key"):
            
            assert_exit(lambda: imgbb_captures_interaction.push_capture_online(image_path))

def test_response_not_json(assert_exit):

    # this test the push_capture_online function with no json. Must exit the program.
    image_path = "image.png" 
    
    with patch("builtins.open", mock_open(read_data=b"fake image data")), \
         patch("requests.post", side_effect = ValueError("Invalid JSON")), \
         patch("os.getenv", return_value="fake_api_key"):
            
            assert_exit(lambda: imgbb_captures_interaction.push_capture_online(image_path))  