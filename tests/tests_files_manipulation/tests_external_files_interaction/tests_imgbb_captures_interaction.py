'''
This tests file concern all functions in the imgbb_captures_interaction module.
It units test the happy path for each function
'''

from unittest.mock import mock_open, patch

from src.predict_core.files_manipulation.external_files_interaction import imgbb_captures_interaction


def test_push_capture_online():

    # this test the push_capture_online function
    image_path = "image.png" 
    expected_url = "https://fakeurl.com/image.png"
    mock_response = type("MockResponse", (), {"json": lambda self: {'data': {'url': expected_url}}})()

    with patch("builtins.open", mock_open(read_data=b"fake image data")), \
         patch("requests.post", return_value=mock_response), \
         patch("os.getenv", return_value="fake_api_key"):
            
            result = imgbb_captures_interaction.push_capture_online(image_path)
            assert result == expected_url