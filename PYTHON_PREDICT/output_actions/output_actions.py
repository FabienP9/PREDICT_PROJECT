'''
    The purpose of this module is to generate message personalized on forums topics.
    This module:
    - choose which message to post (gameday calculated or inited)
    - present some utility functions to run
'''
import logging
logging.basicConfig(level=logging.INFO)
import os
import unicodedata
import re
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.table import Table

import config
import file_actions as fileA
from output_actions import output_actions_sql_queries as sqlQ
from output_actions import output_actions_calculated as outputAC
from output_actions import output_actions_inited as outputAI
from snowflake_actions import snowflake_execute
from imgbb_actions import push_capture_online

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('country','forum')})
def translate_string(content: str, country: str, forum: str) -> str:

    '''
        Translates a text for a given country and a given forum
        Inputs:
            content (str): The text we want to translate
            country (str): The country for which we translate
            forum (str): The forum for which we translate
        Returns:
            The text translated (str)
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    translations = fileA.read_json("output_actions/output_actions_translations.json")
    # We translate for the country...
    country_mapping = translations.get(country, {})
    pattern = re.compile("|".join(map(re.escape, sorted(country_mapping, key=len, reverse=True))))
    content = pattern.sub(lambda m: country_mapping[m.group(0)], content)
    #and for the forum
    forum_mapping = translations.get("FORUM_"+forum, {})
    pattern = re.compile("|".join(map(re.escape, sorted(forum_mapping, key=len, reverse=True))))
    content = pattern.sub(lambda m: forum_mapping[m.group(0)], content)

    return content

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('country','forum')})
def translate_df_headers(df: pd.DataFrame, country: str, forum: str) -> pd.DataFrame:
    
    '''
        Translates df headers for a given country and a given forum
        Inputs:
            df (dataframe): The dataframe for which we want to translate headers
            country (str): The country for which we translate
            forum (str): The forum for which we translate
        Returns:
            The dataframe translated (dataframe)
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    translations = fileA.read_json("output_actions/output_actions_translations.json")
    df = df.rename(columns=translations[country])
    df = df.rename(columns=translations['FORUM_'+forum])

    return df

@config.exit_program(log_filter=lambda args: {})
def format_message(message: str) -> str:

    '''
        Formats a message by:
        - removing original newlines from it
        - replacing each |N| with N*newlines
        Inputs:
            message (str): the message
        Returns:
            The modified string with newlines
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # we remove original newlines
    message = re.sub(r'\n', '', message)

    #subfunction to transform |N| into N* newlines
    def create_newlines(match):
        n = int(match.group(1))
        return '\n' * n
    
    message = re.sub(r'\|(\d+)\|', create_newlines, message)
    return message

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('begin_tag','end_tag','condition')})
def replace_conditionally_message(output_text: str, begin_tag: str, end_tag: str, condition: bool) -> str:

    '''
        Replaces the output template text conditionnally
        If it answers the condition we just remove tags keeping the enclosed block.
        Otherwise we delete all the block with tags
        Inputs:
            output_text (str): the output message text template
            begin_tag (str): the tag beginning the block potentially removed
            end_tag (str): the tag ending the block potentially removed
            condition (boolean): the condition to decide if we remove the block or not
        Returns:
            The output_text modified
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    if condition:
        output_text = output_text.replace(begin_tag, "")
        output_text = output_text.replace(end_tag, "")
    else:
        pattern = f"{re.escape(begin_tag)}.*?{re.escape(end_tag)}"
        output_text = re.sub(pattern, '', output_text, flags=re.DOTALL)
    return output_text

@config.exit_program(log_filter=lambda args: dict(args))
def define_filename(input_type: str, sr_gameday_output_init: pd.Series, extension: str, country: str | None = None, forum: str | None = None) -> str:

    '''
        Creates the name of the output file
        Inputs:
            input_type (str): the type of file (forumoutput_inited, forumoutput_calculated, capture_calculated,...)
            sr_gameday_output_init (series - one row) containing elements for the name
            extension (str): file extension (txt or jpg)
            country (str): the country will be suffixed to the name if provided
            forum (str): the forum name will be suffixed to the name if provided
        Returns:
            The name of the file (str)
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    #subfunction to normalize file name by removing accents, trimming, and lower-case it
    def normalize_string(s):
        # Remove accents
        s = ''.join(
            c for c in unicodedata.normalize('NFD', s)
            if unicodedata.category(c) != 'Mn'
        )
        # Convert to lower-case
        s = s.lower()
        # Remove spaces
        s = s.replace(' ', '')
        return s

    #we calculate the file_name then normalize it
    file_name = input_type + '_' + sr_gameday_output_init['SEASON_ID'] + '_' + sr_gameday_output_init['GAMEDAY'] 
    if country is not None:
        file_name += '_' + country
    if forum is not None:
        file_name += '_' + forum
    file_name += '.' + extension
    file_name = normalize_string(file_name)

    return file_name

@config.exit_program(log_filter=lambda args: {'columns_df': args['df'].columns.tolist(), 'columns': args['columns'] })
def display_rank(df: pd.DataFrame, rank_column: str) -> pd.DataFrame:

    '''
        Sorts a dataframe by its rank - when same rank , replaced with '-' for UI
        Inputs:
            df (dataframe) the dataframe for which we add the rank
            rank_column (str) column used for display the rank
        Returns:
            The dataframe sorted by its rank, on first column
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    df = df.copy()
    df = df.sort_values(by=rank_column, ascending=True)
    df[rank_column] = df[rank_column].astype(int).mask(df.duplicated(subset=rank_column), '-')

    # Move rank to the first position
    col = df.pop(rank_column)    
    df.insert(0, rank_column, col)
    
    return df

@config.exit_program(log_filter=lambda args: {'columns_df': args['df'].columns.tolist(), 'columns': args['columns'] })
def calculate_and_display_rank(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:

    '''
        The purpose of this function is to:
        - add a rank to a dataframe based on given list of columns, sorted descending
        - sort it by this rank - when same rank , replaced with '-' for UI
        Inputs:
            df (dataframe) the dataframe for which we add the rank
            columns (list) list of columns used for calculate the rank
        Returns:
            The dataframe sorted by its rank, on first column
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    df = df.copy()
        
    # Sort and rank
    df = df.sort_values(by=columns, ascending=[False] * len(columns))
    df['RANK'] = df[columns].apply(tuple, axis=1).rank(method='min', ascending=False)
    df = display_rank(df,'RANK')

    return df

@config.exit_program(log_filter=lambda args: {'columns_df': args['df'].columns.tolist(), 'capture_name': args['capture_name'] })
def capture_df_oneheader(df: pd.DataFrame, capture_name: str):

    '''
        Captures a styled jpg using matplotlib from a dataframe with one header level
        Inputs:
            df (dataframe): the dataframe we capture
            capture_name (str): the name of the capture
        Style of the figure:
            applies alternating row colors for readability.
            highlights  headers in bold.
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # color for rows switching background color
    color1 = '#ccd9ff'
    color2 = '#ffffcc'

    fig, ax = plt.subplots(figsize=(5, 0.25*len(df)))
    ax.axis('off')  # Turn off the axes
    table = ax.table(cellText=df.values, colLabels=df.columns, loc='center', cellLoc='center')
    
    # Style the table
    for (row, _), cell in table.get_celld().items():
        cell.set_height(0.07)
        if row == 0:  # Header row
            cell.set_text_props(weight='bold')  # Bold text for column headers
        elif row > 0:  # Data rows (skip the header row)
            # Alternate row colors for readability
            if row % 2 == 0:  # Even row index (data rows)
                cell.set_facecolor(color1) 
            else:  # Odd row index (data rows)
                cell.set_facecolor(color2) 
    
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.auto_set_column_width(range(len(df.columns)))

    # we create the jpg from the figure
    fileA.create_jpg(os.path.join(config.TMPF,capture_name),fig)

@config.exit_program(log_filter=lambda args: {'columns_df': args['df'].columns.tolist(), 'capture_name': args['capture_name'] })
def capture_scores_detailed(df: pd.DataFrame, capture_name: str):

    '''
        Captures a styled jpg using matplotlib from
        the dataframe presenting the detailed scores per user and prediction. 
        It is a two-level header dataframe, so it needs a specific style
        Inputs:
            df (dataframe): the dataframe we capture
            capture_name (str): the name of the capture
        Style of the figure:
            applies alternating row colors for readability
            highlights specific columns and headers in bold
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # Create table data
    header_1 = list(df.columns.get_level_values(0))
    header_2 = list(df.columns.get_level_values(1))
    table_data = [header_2]  # Start with second-level headers
    table_data.insert(0,header_1)  # Add first-level headers
    for row in df.values:
        table_data.append(list(row))

    # Create the figure
    fig, ax = plt.subplots(figsize=(80, 30))  # Adjust figure size
    ax.axis('tight')
    ax.axis('off')

    # colors for row background color switch
    color1 ='#fff2cc'
    color2 ='#ccfff5'

    # Create the table
    tbl = Table(ax, bbox=[0, 0, 1, 1])

    ncols = len(table_data[0])  # Total number of columns

    # Add cells
    for row_idx, row in enumerate(table_data):
        for col_idx, cell in enumerate(row):
            if row_idx == 0 and col_idx > 0:  # Handle merged effect for the first-level header
                if col_idx == 1 or table_data[row_idx][col_idx] != table_data[row_idx][col_idx - 1]:
                    tbl.add_cell(row_idx, col_idx, 1 / ncols, 0.15, text=cell, loc='center', facecolor='white')
                else:
                    # Skip duplicate cells
                    continue
            else:
                height = 0.1 if row_idx > 1 else 0.15  # Adjust height for headers
                tbl.add_cell(row_idx, col_idx, 1 / ncols, height, text=cell, loc='center', facecolor='white')

    # Style the table
    for (row, col), cell in tbl.get_celld().items():
        if row in (0,1):  # Header row
            cell.set_text_props(weight='bold')  # Bold text for column headers
        elif row > 1:  # Data rows (skip the header row)
            # Alternate row colors for readability
            if row % 2 == 0:  # Even row index (data rows)
                cell.set_facecolor(color1) 
            else:  # Odd row index (data rows)
                cell.set_facecolor(color2) 
        #if col == 0:
        #    cell.set_width(0.05)
        if col%6 in (0,1,5):
            cell.set_text_props(weight='bold')
    
    # Add column and row lines to emphasize merged cells
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(30)
    tbl.auto_set_column_width(range(len(df.columns))) # Adjust scale for better visibility

    # Add the table to the plot
    ax.add_table(tbl)

    # we finally create the jpg file
    fileA.create_jpg(os.path.join(config.TMPF,capture_name),fig)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('country','forum','capture_name', 'sr_gameday_output')})
def manage_df(df: pd.DataFrame, country: str, forum: str, capture_name: str, sr_gameday_output: pd.Series) -> str:

    '''
        Manage a dataframe for the output display:
        - translate headers for a given country and forum
        - capture it into a picture
        - send it online
        Inputs:
            df (dataframe): the dataframe we capture
            country(str): the given country
            forum(str): the given forum
            capture_name (str): the short name of the capture
            sr_gameday_output (serie - one row): used to calculate the full name of the capture
            
        Returns:
            the url of the capture online (str)    
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    df = translate_df_headers(df, country, forum)
    full_capture_name = define_filename(capture_name, sr_gameday_output, 'jpg', country, forum)
    if df.columns.nlevels == 1:
       capture_df_oneheader(df, full_capture_name) 
    else:
       capture_scores_detailed(df, full_capture_name) 
    local_path = os.path.join(config.TMPF, full_capture_name)
    url = push_capture_online(local_path)

    return url

@config.exit_program(log_filter=lambda args: {})
def generate_output_message(context_dict: dict):

    '''
        Generates and posts messages on forums.It:
        - checks if an inited or calculated message should be posted 
        - calls the process for generating and post the message
        Input:
            context_dict (dict): The data dictionary to decide what needs to be posted
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # we personalize the query and get the output need gameday
    sr_output_need = context_dict['sr_output_need']
    qGamedayOutput = sqlQ.qGamedayOutput
    df_gameday_output = snowflake_execute(context_dict['sr_snowflake_account_connect'],qGamedayOutput,(sr_output_need['SEASON_ID'],sr_output_need['GAMEDAY']))
    
    has_to_post_inited_message = False
    has_to_post_calculated_message = False
    
    #We choose what needs to be posted based on TASK_RUN
    if len(df_gameday_output) > 0 and sr_output_need['TASK_RUN']  == config.TASK_RUN_MAP["INIT"]:
        has_to_post_inited_message = True
    
    if len(df_gameday_output) > 0 and sr_output_need['TASK_RUN']  == config.TASK_RUN_MAP["CALCULATE"]:
        has_to_post_calculated_message = True

    if len(df_gameday_output) > 0:
        #if the process deleted calculation we post also calculated message as it might change rankings
        if sr_output_need['IS_TO_DELETE'] == 1:
            has_to_post_calculated_message = True

    if has_to_post_inited_message:
        outputAI.process_output_message_inited(context_dict,df_gameday_output.iloc[0])

    if has_to_post_calculated_message:
       outputAC.process_output_message_calculated(context_dict,df_gameday_output.iloc[0])
