'''
This tests file concern all functions in the playoffs_table_generation module.
It units test the happy path for each function
'''

from unittest.mock import patch
import matplotlib.pyplot as plt

from src.predict_core.entry_point import playoffs_table_generation

def test_get_matchups_strings():
    
    # this test the get_matchups_strings function
    playoffs_matchups = [["Team A", "Team B"], ["Team C", "Team D"]]
    expected = ["Team A\nTeam B", "Team C\nTeam D"]
    result = playoffs_table_generation.get_matchups_strings(playoffs_matchups)
    assert result == expected

def test_get_results_strings_with_scores():
    
    # this test the get_results_strings function with scores non 0
    playoffs_results = [["2", "1"], ["0", "1"]]
    result = playoffs_table_generation.get_results_strings(playoffs_results)
    expected = ["2\n1", "0\n1"]
    assert result == expected

def test_get_results_strings_with_zeros():
    
    # this test the get_results_strings function with scores non 0
    playoffs_results = [["0", "0"], ["1", "0"]]
    result = playoffs_table_generation.get_results_strings(playoffs_results)
    expected = ["", "1\n0"]
    assert result == expected

def test_display_textbox_without_results():
    
    # this test the display_textbox function without result
    _, ax = plt.subplots()
    column = 1
    line = 2
    str_matchup = "Team A\nTeam B"
    str_result = ""
    ax_out = playoffs_table_generation.display_textbox(ax, column, line, str_matchup, str_result)
    assert isinstance(ax_out, type(ax))

def test_display_textbox_with_results():
    
    # this test the display_textbox function with result
    _, ax = plt.subplots()
    column = 1
    line = 2
    str_matchup = "Team A\nTeam B"
    str_result = "1\n0"
    ax_out = playoffs_table_generation.display_textbox(ax, column, line, str_matchup, str_result)
    assert isinstance(ax_out, type(ax))

def test_draw_line():
    
    # this test the draw_line function
    _, ax = plt.subplots()
    column1 = 1
    column2 = 2
    line1 = 3
    line2 = 4
    ax_out = playoffs_table_generation.draw_line(ax, column1, column2, line1, line2)
    assert isinstance(ax_out, type(ax))

def test_display_pass():
    
    # this test the display_pass function
    _, ax = plt.subplots()
    column = 1
    line = 2
    passvalue = "WIN *2"

    ax_out = playoffs_table_generation.display_pass(ax, column, line, passvalue)
    assert isinstance(ax_out, type(ax))

def test_draw_playoffs_image(read_txt):
    
    # this test the draw_playoffs_image function
    mock_str_playoffs_table = read_txt("playoffs_table.txt")

    with patch.object(playoffs_table_generation.local_environment_manipulation,"create_local_folder"), \
         patch.object(playoffs_table_generation.dropbox,"download_file", return_value = {"str_playoffs_table": mock_str_playoffs_table}), \
         patch.object(playoffs_table_generation.files_manipulation,"create_jpg"), \
         patch.object(playoffs_table_generation.imgbb,"push_capture_online", return_value="https://fakeimage.url/test.jpg"), \
         patch.object(playoffs_table_generation,"create_json_file_email"), \
         patch.object(playoffs_table_generation.local_environment_manipulation,"destroy_local_folder"):

        playoffs_table_generation.draw_playoffs_image()
