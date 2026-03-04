'''
This tests file concern all functions in the exe_playoffs_table module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch, mock_open
import matplotlib.pyplot as plt
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'PYTHON_PREDICT'))
sys.path.insert(0, ROOT)
import exe_playoffs_table
from testutils import read_txt

def test_get_matchups_strings():
    
    # this test the get_matchups_strings function
    playoffs_matchups = [["Team A", "Team B"], ["Team C", "Team D"]]
    expected = ["Team A\nTeam B", "Team C\nTeam D"]
    result = exe_playoffs_table.get_matchups_strings(playoffs_matchups)
    unittest.TestCase().assertEqual(result, expected)

def test_get_results_strings_with_scores():
    
    # this test the get_results_strings function with scores non 0
    playoffs_results = [["2", "1"], ["0", "1"]]
    result = exe_playoffs_table.get_results_strings(playoffs_results)
    expected = ["2\n1", "0\n1"]
    unittest.TestCase().assertEqual(result, expected)

def test_get_results_strings_with_zeros():
    
    # this test the get_results_strings function with scores non 0
    playoffs_results = [["0", "0"], ["1", "0"]]
    result = exe_playoffs_table.get_results_strings(playoffs_results)
    expected = ["", "1\n0"]
    unittest.TestCase().assertEqual(result, expected)

def test_display_textbox_without_results():
    
    # this test the display_textbox function without result
    _, ax = plt.subplots()
    column = 1
    line = 2
    str_matchup = "Team A\nTeam B"
    str_result = ""
    ax_out = exe_playoffs_table.display_textbox(ax, column, line, str_matchup, str_result)
    unittest.TestCase().assertIsInstance(ax_out, type(ax))

def test_display_textbox_with_results():
    
    # this test the display_textbox function with result
    _, ax = plt.subplots()
    column = 1
    line = 2
    str_matchup = "Team A\nTeam B"
    str_result = "1\n0"
    ax_out = exe_playoffs_table.display_textbox(ax, column, line, str_matchup, str_result)
    unittest.TestCase().assertIsInstance(ax_out, type(ax))

def test_draw_line():
    
    # this test the draw_line function
    _, ax = plt.subplots()
    column1 = 1
    column2 = 2
    line1 = 3
    line2 = 4
    ax_out = exe_playoffs_table.draw_line(ax, column1, column2, line1, line2)
    unittest.TestCase().assertIsInstance(ax_out, type(ax))

def test_display_pass():
    
    # this test the display_pass function
    _, ax = plt.subplots()
    column = 1
    line = 2
    passvalue = "WIN *2"

    ax_out = exe_playoffs_table.display_pass(ax, column, line, passvalue)
    unittest.TestCase().assertIsInstance(ax_out, type(ax))

def test_draw_playoffs_image():
    
    # this test the draw_playoffs_image function
    mock_str_playoffs_table = read_txt("materials/playoffs_table.txt")

    with patch.object(exe_playoffs_table.config,"create_local_folder"), \
         patch.object(exe_playoffs_table,"download_file", return_value = {"str_playoffs_table": mock_str_playoffs_table}), \
         patch.object(exe_playoffs_table,"create_jpg"), \
         patch.object(exe_playoffs_table,"push_capture_online", return_value="https://fakeimage.url/test.jpg"), \
         patch("builtins.open", new_callable=mock_open, read_data=b"fake image data"), \
         patch.object(exe_playoffs_table.config,"destroy_local_folder"):

        exe_playoffs_table.draw_playoffs_image()

if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_matchups_strings))
    test_suite.addTest(unittest.FunctionTestCase(test_get_results_strings_with_scores))
    test_suite.addTest(unittest.FunctionTestCase(test_get_results_strings_with_zeros))
    test_suite.addTest(unittest.FunctionTestCase(test_display_textbox_without_results))
    test_suite.addTest(unittest.FunctionTestCase(test_display_textbox_with_results))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_line))
    test_suite.addTest(unittest.FunctionTestCase(test_display_pass))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
