'''
This tests file concern all functions in the exe_playoffs_table module.
It units test unexpected path for each function
'''
import unittest
from unittest.mock import patch,mock_open
import matplotlib.pyplot as plt
import os
import sys
import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import exe_playoffs_table
from testutils import assertExit
from testutils import read_txt

def test_get_matchups_strings_empty_list():
    
    # this test the get_matchups_strings function with empty list. Must return empty list
    playoffs_matchups = []
    expected = []
    result = exe_playoffs_table.get_matchups_strings(playoffs_matchups)
    unittest.TestCase().assertEqual(result, expected)
    
    assert exe_playoffs_table.get_matchups_strings([]) == []

def test_get_matchups_strings_invalid_type():
    
    # this test the get_matchups_strings function with list with invalid types. Must exit the program
    playoffs_matchups = [None]
    assertExit(lambda: exe_playoffs_table.get_matchups_strings(playoffs_matchups))

def test_get_results_strings_all_zero_results():
    
    # this test the get_results_strings function with only zeros results. Must return an empty string
    playoffs_results = [["0", "0", "0"]]
    result = exe_playoffs_table.get_results_strings(playoffs_results)
    expected = [""]
    unittest.TestCase().assertEqual(result, expected)

def test_get_results_strings_invalid_type():
    
    # this test the get_results_strings function with invalid type. Must exit the program
    playoffs_results = [[[123, 456]]]
    assertExit(lambda: exe_playoffs_table.get_results_strings(playoffs_results))

def test_display_textbox_with_result_adds_artist():
    
    # this test the display_textbox function with one artist
    _, ax = plt.subplots()
    column = 1
    line = 2
    str_matchup = "Team A\nTeam B"
    str_result = "1\n0"
    ax_out = exe_playoffs_table.display_textbox(ax, column, line, str_matchup, str_result)
    assert len(ax_out.artists) == 1

def test_display_textbox_without_result_adds_text():
    
    # this test the display_textbox function without results
    _, ax = plt.subplots()
    column = 1
    line = 2
    str_matchup = "Team A\nTeam B"
    str_result = ""
    ax_out = exe_playoffs_table.display_textbox(ax, column, line, str_matchup, str_result)
    assert any("Team A" in t.get_text() for t in ax_out.texts)

def test_draw_line_and_display_pass():
    
    # this test the draw_line and display_pass function
    _, ax = plt.subplots()
    column1 = 1
    column2 = 2
    line1 = 3
    line2 = 4
    
    column = 1
    line = 2
    passvalue = "WIN *2"

    ax_out = exe_playoffs_table.draw_line(ax, column1, column2, line1, line2)
    ax_out = exe_playoffs_table.display_pass(ax, column, line, passvalue)
    assert len(ax_out.lines) == 1
    assert any("WIN *2" in t.get_text() for t in ax_out.texts)

def test_draw_playoffs_image_download_fails():
    
    # this test the draw_playoffs_image function, with an error in download_file fails. Must exit the program
    with patch.object(exe_playoffs_table.config,"create_local_folder"), \
         patch.object(exe_playoffs_table,"download_file", side_effect=Exception("dropbox error")):

        assertExit(lambda: exe_playoffs_table.draw_playoffs_image())

def test_draw_playoffs_image_exec_fails():

    # this test the draw_playoffs_image function, with an invalid code read in exe. Must exit the program
    mock_str_playoffs_table = "invalid_code"

    with patch.object(exe_playoffs_table.config,"create_local_folder"), \
         patch.object(exe_playoffs_table,"download_file", return_value = {"str_playoffs_table": mock_str_playoffs_table}), \
         patch.object(exe_playoffs_table,"create_jpg"), \
         patch.object(exe_playoffs_table,"push_capture_online", return_value="https://fakeimage.url/test.jpg"), \
         patch("builtins.open", new_callable=mock_open, read_data=b"fake image data"), \
         patch.object(exe_playoffs_table.config,"destroy_local_folder"):

         assertExit(lambda: exe_playoffs_table.draw_playoffs_image())

def test_draw_playoffs_image_create_jpg_fails():
    
    # this test the draw_playoffs_image function, with create_jpg failing. Must exit the program
    mock_str_playoffs_table = read_txt("materials/playoffs_table.txt")

    with patch.object(exe_playoffs_table.config,"create_local_folder"), \
         patch.object(exe_playoffs_table,"download_file", return_value = {"str_playoffs_table": mock_str_playoffs_table}), \
         patch.object(exe_playoffs_table,"create_jpg", side_effect=Exception("create_jpg failed")):

        assertExit(lambda: exe_playoffs_table.draw_playoffs_image())

def test_draw_playoffs_image_imgbb_bad_json():
    
    # this test the draw_playoffs_image function, with bad json for pushing image online. Must exit the program
    mock_str_playoffs_table = read_txt("materials/playoffs_table.txt")

    with patch.object(exe_playoffs_table.config,"create_local_folder"), \
         patch.object(exe_playoffs_table,"download_file", return_value = {"str_playoffs_table": mock_str_playoffs_table}), \
         patch.object(exe_playoffs_table,"create_jpg"), \
         patch("builtins.open", new_callable=mock_open, read_data=b"fake image data"), \
         patch.object(requests,"post") as mock_post, \
         patch.object(exe_playoffs_table.config,"destroy_local_folder"):

        mock_post.return_value.json = lambda: {"bad": "json"}
        assertExit(lambda: exe_playoffs_table.draw_playoffs_image())

if __name__ == "__main__":
    test_suite = unittest.TestSuite()

    test_suite.addTest(unittest.FunctionTestCase(test_get_matchups_strings_empty_list))
    test_suite.addTest(unittest.FunctionTestCase(test_get_matchups_strings_invalid_type))
    test_suite.addTest(unittest.FunctionTestCase(test_get_results_strings_all_zero_results))
    test_suite.addTest(unittest.FunctionTestCase(test_get_results_strings_invalid_type))
    test_suite.addTest(unittest.FunctionTestCase(test_display_textbox_with_result_adds_artist))
    test_suite.addTest(unittest.FunctionTestCase(test_display_textbox_without_result_adds_text))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_line_and_display_pass))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image_download_fails))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image_exec_fails))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image_create_jpg_fails))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image_imgbb_bad_json))

    runner = unittest.TextTestRunner()
    runner.run(test_suite)