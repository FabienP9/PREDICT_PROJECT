'''
This tests file concern all functions in the playoffs_table_generation module.
It units test unexpected path
'''
from unittest.mock import patch
import matplotlib.pyplot as plt
import requests

from src.predict_core.entry_point import playoffs_table_generation

def test_get_matchups_strings_empty_list():
    
    # this test the get_matchups_strings function with empty list. Must return empty list
    playoffs_matchups = []
    expected = []
    result = playoffs_table_generation.get_matchups_strings(playoffs_matchups)
    assert result == expected
    assert playoffs_table_generation.get_matchups_strings([]) == []

def test_get_matchups_strings_invalid_type(assert_exit):
    
    # this test the get_matchups_strings function with list with invalid types. Must exit the program
    playoffs_matchups = [None]
    assert_exit(lambda: playoffs_table_generation.get_matchups_strings(playoffs_matchups))

def test_get_results_strings_all_zero_results():
    
    # this test the get_results_strings function with only zeros results. Must return an empty string
    playoffs_results = [["0", "0", "0"]]
    result = playoffs_table_generation.get_results_strings(playoffs_results)
    expected = [""]
    assert result == expected

def test_get_results_strings_invalid_type(assert_exit):
    
    # this test the get_results_strings function with invalid type. Must exit the program
    playoffs_results = [[[123, 456]]]
    assert_exit(lambda: playoffs_table_generation.get_results_strings(playoffs_results))

def test_display_textbox_with_result_adds_artist():
    
    # this test the display_textbox function with one artist
    _, ax = plt.subplots()
    column = 1
    line = 2
    str_matchup = "Team A\nTeam B"
    str_result = "1\n0"
    ax_out = playoffs_table_generation.display_textbox(ax, column, line, str_matchup, str_result)
    assert len(ax_out.artists) == 1

def test_display_textbox_without_result_adds_text():
    
    # this test the display_textbox function without results
    _, ax = plt.subplots()
    column = 1
    line = 2
    str_matchup = "Team A\nTeam B"
    str_result = ""
    ax_out = playoffs_table_generation.display_textbox(ax, column, line, str_matchup, str_result)
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

    playoffs_table_generation.draw_line(ax, column1, column2, line1, line2)
    ax_out = playoffs_table_generation.display_pass(ax, column, line, passvalue)
    assert len(ax_out.lines) == 1
    assert any("WIN *2" in t.get_text() for t in ax_out.texts)

def test_draw_playoffs_image_download_fails(assert_exit):
    
    # this test the draw_playoffs_image function, with an error in download_file fails. Must exit the program
    with patch.object(playoffs_table_generation.local_environment_manipulation,"create_local_folder"), \
         patch.object(playoffs_table_generation.dropbox,"download_file", side_effect=Exception("dropbox error")):

        assert_exit(lambda: playoffs_table_generation.draw_playoffs_image())

def test_draw_playoffs_image_exec_fails(assert_exit):

    # this test the draw_playoffs_image function, with an invalid code read in execution. Must exit the program
    mock_str_playoffs_table = "invalid_code"

    with patch.object(playoffs_table_generation.local_environment_manipulation,"create_local_folder"), \
         patch.object(playoffs_table_generation.dropbox,"download_file", return_value = {"str_playoffs_table": mock_str_playoffs_table}), \
         patch.object(playoffs_table_generation.files_manipulation,"create_jpg"), \
         patch.object(playoffs_table_generation.imgbb,"push_capture_online", return_value="https://fakeimage.url/test.jpg"), \
         patch.object(playoffs_table_generation,"create_json_file_email"), \
         patch.object(playoffs_table_generation.local_environment_manipulation,"destroy_local_folder"):

         assert_exit(lambda: playoffs_table_generation.draw_playoffs_image())

def test_draw_playoffs_image_create_jpg_fails(read_txt, assert_exit):
    
    # this test the draw_playoffs_image function, with create_jpg failing. Must exit the program
    mock_str_playoffs_table = read_txt("playoffs_table.txt")

    with patch.object(playoffs_table_generation.local_environment_manipulation,"create_local_folder"), \
         patch.object(playoffs_table_generation.dropbox,"download_file", return_value = {"str_playoffs_table": mock_str_playoffs_table}), \
         patch.object(playoffs_table_generation.files_manipulation,"create_jpg", side_effect=Exception("create_jpg failed")):

        assert_exit(lambda: playoffs_table_generation.draw_playoffs_image())

def test_draw_playoffs_image_imgbb_bad_json(read_txt, assert_exit):
    
    # this test the draw_playoffs_image function, with bad json for pushing image online. Must exit the program
    mock_str_playoffs_table = read_txt("playoffs_table.txt")

    with patch.object(playoffs_table_generation.local_environment_manipulation,"create_local_folder"), \
         patch.object(playoffs_table_generation.dropbox,"download_file", return_value = {"str_playoffs_table": mock_str_playoffs_table}), \
         patch.object(playoffs_table_generation.files_manipulation,"create_jpg"), \
         patch.object(playoffs_table_generation,"create_json_file_email"), \
         patch.object(requests,"post") as mock_post, \
         patch.object(playoffs_table_generation.local_environment_manipulation,"destroy_local_folder"):

        mock_post.return_value.json = lambda: {"bad": "json"}
        assert_exit(lambda: playoffs_table_generation.draw_playoffs_image())
