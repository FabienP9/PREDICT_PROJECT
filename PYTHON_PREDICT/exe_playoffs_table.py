'''
This module is an entry point of the program, it runs the draw_playoffs_image function
'''
import logging
logging.basicConfig(level=logging.INFO)
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from PIL import Image
import numpy as np
from matplotlib.offsetbox import TextArea, HPacker, AnnotationBbox
import os
from datetime import datetime,timezone
import json

import config
from file_actions.file_actions import create_jpg
from dropbox_actions import download_file
from output_message_generation_actions.imgbb_actions import push_capture_online

@config.exit_program(log_filter=lambda args: {})
def get_matchups_strings(playoffs_matchups: list[list[str]]) -> list[str]:

    '''
        Get all match-ups strings into a list from the list of teams
        Args:
            playoffs_matchups (list) : Contains all teams and their matchups in playoffs
        Returns:
            list: the list of strings for each matchup
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    str_matchups = []
    for round_teams in playoffs_matchups:
            str_matchups.append('\n'.join(round_teams))
    return str_matchups

@config.exit_program(log_filter=lambda args: {})
def get_results_strings(playoffs_results: list[list[str]]) -> list[str]:
    
    '''
        Get all results strings into a list from the list of results
        Args:
            playoffs_results (list) : Contains all results
        Returns:
            list: the list of strings for each result
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    str_results = []
    for round_results in playoffs_results:
        if all(r == '0' for r in round_results):
            str_results.append('')
        else:
            str_results.append('\n'.join(round_results))
    return str_results

@config.exit_program(log_filter=lambda args: {})
def display_textbox(ax: Axes, column: int, line: int, str_matchup: str, str_result: str,size: int = 13) -> Axes:

    '''
        Create text box on the figure with matchups and results
        Args:
            ax (matplot lib axes) : The axes of the figure
            column (int): the column where the text box is displayed
            line (int): the line where the text box is displayed
            str_matchup (str): the text of matchup inside the box
            str_result (str): the text of results inside the box
            size (int): the size of text font inside the box
        Returns:
            matplot lib axes: the axe updated
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # the type of box depends on the presence of results
    if str_result == "":
        team_box = ax.text(column+0.2,line,str_matchup, fontsize=size, fontweight="bold", ha="left",bbox=dict(facecolor='white', edgecolor='black'),zorder=10)
    else:
        team_box = TextArea(str_matchup, textprops=dict(fontsize=size, fontweight="bold"))
        result_box = TextArea(str_result, textprops=dict(fontsize=size, fontweight="bold", fontstyle="italic"))
        packed_box = HPacker(children=[team_box, result_box], align="left", pad=0, sep=6)
        ab = AnnotationBbox(
            packed_box, 
            (column+0.2,line),
            xycoords='data',
            boxcoords="offset points",
            box_alignment=(0, 0.5),
            frameon=True,
            bboxprops=dict(facecolor='white', edgecolor='black', boxstyle='round,pad=0.3'),
            zorder=10)
        ax.add_artist(ab)

    return ax

@config.exit_program(log_filter=lambda args: {})
def draw_line(ax: Axes, column1: int, column2: int, line1: int, line2: int) -> Axes:

    '''
        Draw a line in the image
        Args:
            ax (matplot lib axes) : The axes of the figure
            column1 (int): the column where the line start
            column2 (int): the column where the line ends
            line1 (int): the line where the line start
            line2 (int): the line where the line ends
        Returns:
            matplotlib axes: the axe updated
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    ax.plot([column1+0.25, column2+0.25], [line1+0.25, line2+0.25], 'k-', linewidth=1.5, zorder=8)
    return ax

@config.exit_program(log_filter=lambda args: {})
def display_pass(ax: Axes, column: int, line: int, passvalue: str) -> Axes:

    '''
        Display pass values on the image
        Args:
            ax (matplot lib axes) : The axes of the figure
            column (int): the column where the pass value is written
            line (int): the line where the pass value is written
            passvalue (str): pass value to write
        Returns:
            matplotlib axes: the axe updated
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    ax.text(column, line, passvalue, ha='center', va='bottom', fontsize=8, fontweight="bold", style='italic',zorder=9)
    return ax

@config.exit_program(log_filter=lambda args: {})
def draw_playoffs_image():

    '''
        This function can be called directly by the user or GitHub action
        It draws a playoffs image and push it on ImgBB.
        It is mainly hardcoded as every textbox and line is personalized uniquely 
        based on how readable it is when displayed on the image.
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    #We create local folders to store inputs
    config.create_local_folder()
    context_dict = {}
    # we download lists to generate image from dropbox
    context_dict.update(download_file(dropbox_file_path = config.playoffs_table_code,
                                local_folder = config.TMPF,
                                is_encapsulated=0,
                                is_path_abs=1))
    exec_dict = {}
    exec(context_dict['str_playoffs_table'], exec_dict)
    playoffs_matchups = exec_dict['playoffs_matchups']
    s = exec_dict['s']
    playoffs_results = exec_dict['playoffs_results']
    playoffs_title = exec_dict['playoffs_title']
    playoffs_round = exec_dict['playoffs_round']
    playoffs_message = exec_dict['playoffs_message']
    playoffs_passvalues = exec_dict['playoffs_passvalues']

    # we initiate the figure
    fig, ax = plt.subplots(figsize=(20, 12))
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 12)
    ax.axis("off")

    # if the playoffs matchups is completely filled: we have a winner, so we download trophy.jpg and fill the final image with it
    if (playoffs_matchups[len(playoffs_matchups)-1][0]) != s:

        download_file(dropbox_file_path = config.trophy_file_path,
                                        local_folder = config.TMPF,
                                        is_encapsulated=0,
                                        is_path_abs=1)
        
        
        trophy_img = Image.open(config.TMPF+"/Trophy.JPG").convert('RGBA')
        trophy_img = np.array(trophy_img)

        # we position the trophy on the image
        lin_img_1 = 4.65
        lin_img_2 = 10.5
        col_img_1 = 14
        col_img_2 = 18.75
        ax.imshow(trophy_img, extent=[col_img_1, col_img_2, lin_img_1, lin_img_2 ], 
            aspect='auto', zorder=5) 

    # We add titles and messages
    ax.text(6, 12, playoffs_title, fontsize=18, fontweight="bold", ha="center")
    for round,_ in enumerate(playoffs_round):
        ax.text(1+3*round,11,playoffs_round[round], fontsize=16, fontweight="bold", ha="left")
    ax.text(1, 1, playoffs_message, fontsize=11, fontstyle="italic", ha="left")

    # we create matchups strings
    str_matchups = get_matchups_strings(playoffs_matchups)

    # we create result strings
    str_results = get_results_strings(playoffs_results)

    # on the following we create text box and draw lines
    #Plays-In Round 1 - 9v10 to PI2
    column1 = 1
    line1 = 7
    ax = display_textbox(ax,column1, line1, str_matchups[0], str_results[0])
    column2 = 3.75
    line2 = 7
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[0])
    column3 = 3.75
    line3 = 6
    ax = draw_line(ax,column2,column3,line2,line3)
    column4 = 4
    line4 = 6
    ax = draw_line(ax,column3,column4,line3,line4)

    #Plays-In Round 1 - 7v8 to PI2
    column1 = 1
    line1 = 5
    ax = display_textbox(ax,column1, line1, str_matchups[1], str_results[1])
    column2 = 3.75
    line2 = 5
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.3,playoffs_passvalues[1])
    column3 = 3.75
    line3 = 6
    ax = draw_line(ax,column2,column3,line2,line3)
    column4 = 4
    line4 = 6
    ax = draw_line(ax,column3,column4,line3,line4)

    #Plays-In Round 1 - 7v8 to QDF
    column1 = 1
    line1 = 4.8
    column2 = 4
    line2 = 4.8
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[2])
    column3 = 4
    line3 = 5
    ax = draw_line(ax,column2,column3,line2,line3)
    column4 = 7
    line4 = 5
    ax = draw_line(ax,column3,column4,line3,line4)

    #Plays-In Round 2 to QDF
    column1 = 4
    line1 = 6
    ax = display_textbox(ax,column1, line1, str_matchups[2], str_results[2])
    column2 = 6.8
    line2 = 6
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[3])
    column3 = 6.8
    line3 = 9
    ax = draw_line(ax,column2,column3,line2,line3)
    column4 = 7
    line4 = 9
    ax = draw_line(ax,column3,column4,line3,line4)

    #Quarter-Finals 1vsPI2 to semi-final
    column1 = 7
    line1 = 9
    ax = display_textbox(ax,column1, line1, str_matchups[3], str_results[3])
    column2 = 9.8
    line2 = 9
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[4])
    column3 = 9.8
    line3 = 7
    ax = draw_line(ax,column2,column3,line2,line3)
    column4 = 10
    line4 = 7
    ax = draw_line(ax,column3,column4,line3,line4)

    #Quarter-Finals 4vs5 to semi-final
    column1 = 7
    line1 = 7
    ax = display_textbox(ax,column1, line1, str_matchups[4], str_results[4])
    column2 = 9.8
    line2 = 7
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[5])
    column3 = 10
    line3 = 7
    ax = draw_line(ax,column2,column3,line2,line3)

    #Quarter-Finals 2vsPI1 to semi-final
    column1 = 7
    line1 = 5
    ax = display_textbox(ax,column1, line1, str_matchups[5], str_results[5])
    column2 = 9.8
    line2 = 5
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[6])
    column3 = 10
    line3 = 5
    ax = draw_line(ax,column2,column3,line2,line3)

    #Quarter-Finals 3vs6 to semi-final
    column1 = 7
    line1 = 3
    ax = display_textbox(ax,column1, line1, str_matchups[6], str_results[6])
    column2 = 9.8
    line2 = 3
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[7])
    column3 = 9.8
    line3 = 5
    ax = draw_line(ax,column2,column3,line2,line3)
    column4 = 10
    line4 = 5
    ax = draw_line(ax,column3,column4,line3,line4)

    #Semi-Final QF1vsQF2 to Final
    column1 = 10
    line1 = 7
    ax = display_textbox(ax,column1, line1, str_matchups[7], str_results[7])
    column2 = 12.8
    line2 = 7
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[8])
    column3 = 12.8
    line3 = 6
    ax = draw_line(ax,column2,column3,line2,line3)
    column4 = 13
    line4 = 6
    ax = draw_line(ax,column3,column4,line3,line4)

    #Semi-Final QF3vsQF4 to Final
    column1 = 10
    line1 = 5
    ax = display_textbox(ax,column1, line1, str_matchups[8], str_results[8])
    column2 = 12.8
    line2 = 5
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[9])
    column3 = 12.8
    line3 = 6
    ax = draw_line(ax,column2,column3,line2,line3)
    column4 = 13
    line4 = 6
    ax = draw_line(ax,column3,column4,line3,line4)

    #Final to Champions
    column1 = 13
    line1 = 6
    ax = display_textbox(ax,column1, line1, str_matchups[9], str_results[9])
    column2 = 16
    line2 = 6
    ax = draw_line(ax,column1,column2,line1,line2)
    ax = display_pass(ax,column2-0.05,line2+0.05,playoffs_passvalues[10])
    column3 = 16
    line3 = 5.5
    ax = draw_line(ax,column2,column3,line2,line3)

    #Champions
    column1 = 16
    line1 = 5.5
    ax = display_textbox(ax,column1, line1, str_matchups[10], str_results[10],16)

    # we create the jpg file
    file_path = os.path.join(config.TMPF,"playoffs_table_"+datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S")+".jpg")
    create_jpg(file_path,fig)

    # we push it online on ImgBB
    image_url = push_capture_online(file_path)
    logging.info(image_url)
    
    with open("exe_output.json", "w") as f:
        json.dump({
            "image_url": image_url
        }, f) 

    # We finally destroy local folders
    config.destroy_local_folder()

# Generate the playoffs image
if __name__ == "__main__":
    draw_playoffs_image()