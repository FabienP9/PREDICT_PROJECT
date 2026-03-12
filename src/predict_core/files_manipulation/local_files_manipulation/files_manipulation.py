'''
    The purpose of this module is to interact with files by
     - creating and terminating local folders which will store them temporarily
'''
import logging
import os
import pandas as pd
import json
from pathlib import Path
from typing import Literal
import csv
from matplotlib.figure import Figure
import networkx as nx

from ...config import config_decorators
from ...config.config_variables import config_global_variables as var

logging.basicConfig(level=logging.INFO)

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def read_json(local_file_path: str) -> list :

    """
        Reads the list from a json file and returns the content
        Args:
            local_file_path (str): Local path to the json file
        Returns:
            The content of the json file (list)
        Raises:
            Exits the program if error running the function (using decorator)
    """

    with open(local_file_path, 'r', encoding='utf-8') as file:
        lst = json.load(file)
    return lst

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def read_and_check_csv(local_file_path: str, is_encapsulated: Literal[0, 1] = 0) -> pd.DataFrame:
    """
        Calls the read_csv function from pandas and return the dataframe
        if all expected columns are there
        Args:
            local_file_path (str): Local path to the csv file
            is_encapsulated (0/1): Has the file been encapsulated (with ")? 1= yes, 0=no - default = no
        Returns:
            The dataframe of the csv
        Raises:
            Exits the program if error running the function or if columns not found (using decorator)
    """
    if is_encapsulated==1:
        df = pd.read_csv(local_file_path,header=0,quotechar='"')
    else:
        df = pd.read_csv(local_file_path,header=0)

    filename = Path(local_file_path).name
    expected_columns = read_json(Path(__file__).resolve().parent / "file_check.json")["schemas"].get(filename, {}).get("columns", {}) # NOSONAR
    actual_columns = df.columns.tolist()
    missing = [col for col in expected_columns if col not in actual_columns]
    if missing:
        raise ValueError(f"Columns missing in {filename}: {missing}")

    type_mismatches = []
    for col, expected_type in expected_columns.items():
        actual_type = str(df[col].dtype)

        normalized_actual = "object" if actual_type in ["object", "str"] else actual_type
        normalized_expected = "object" if expected_type in ["object", "str"] else expected_type
        
        if df[col].isna().all() and normalized_expected == "object":
            continue  

        if normalized_actual != normalized_expected:
            type_mismatches.append((col, expected_type, actual_type))

    if type_mismatches:
        mismatch_msgs = [f"{col}: expected {exp}, got {act}" for col, exp, act in type_mismatches]
        raise ValueError(f"Type mismatches in {filename}: {mismatch_msgs}")
    
    return df
    
@config_decorators.exit_program(log_filter=lambda args: dict(args))
def read_yml(local_file_path: str) -> str:
    """
        Reads the text from a yml file and returns the content in str format
        Args:
            local_file_path (str): Local path to the YML file
        Returns:
            The content of the yaml file (str)
        Raises:
            Exits the program if error running the function (using decorator)
    """
    content = ""
    with open(local_file_path, 'r', encoding='utf-8') as file:
        content = file.read() 
    return content

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def read_txt(local_file_path: str) -> str:
    """
        Reads the text from a txt file and returns the content
        Args:
            local_file_path (str): Local path to the txt file
        Returns:
            The content of the txt file (str)
        Raises:
            Exits the program if error running the function (using decorator)
    """
    content = ""
    with open(local_file_path, 'r', encoding='utf-8') as file:
        content = file.read() 
    return content

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('local_file_path', 'is_to_encapsulate') })
def create_csv(local_file_path: str ,df: pd.DataFrame, is_to_encapsulate: Literal[0, 1] = 0):

    """
        The purpose of this function is to create a csv file using the pandas to_csv method    
        Args:
            local_file_path (str): Path where the CSV file will be saved
            df (dataframe): DataFrame to write to CSV
            is_to_encapsulate (0/1): If 1, encapsulate fields with "". Default is 0 (no encapsulation)
        Raises:
            Exits the program if error running the function (using decorator)
    """
    if is_to_encapsulate == 1:
        df.to_csv(local_file_path , index=False, quotechar='"', quoting=csv.QUOTE_ALL, encoding='utf-8', header=True)
    else:
        df.to_csv(local_file_path, index=False, encoding='utf-8',header=True)

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('local_file_path',) })
def create_yml(local_file_path: str, text: str):

    """
        Creates a YAML file from a string (already yml-formatted)
        Args:
            local_file_path (str) : The local path of the file
            text (str) : The string to write
        Raises:
            Exits the program if error running the function (using decorator)
    """
    with open(local_file_path, "w", encoding = "utf-8") as file:
        file.write(text)

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('local_file_path',) })
def create_txt(local_file_path: str, text: str):
    """
        Creates a text file from a string
        Args:
            local_file_path (str) : The local path of the file
            text (str) : The string to write
        Raises:
            Exits the program if error running the function (using decorator)
    """
    with open(local_file_path, "w", encoding="utf-8") as file:
        file.write(text)

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('local_file_path',) })
def create_jpg(local_file_path: str, fig: Figure):

    """
        Creates a jpg file from a figure
        Args:
            local_file_path (str) : The local path of the file
            fig (matplotlib figure) : The figure to write
        Raises:
            Exits the program if error running the function (using decorator)
    """
    fig.tight_layout()
    fig.savefig(local_file_path, facecolor=fig.get_facecolor(), format='jpg', dpi=150, bbox_inches='tight')

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('filtering_category',) })
def filter_data(files_data_dict: dict, df_paths: pd.DataFrame, filtering_category: str) -> dict:

    '''
    The purpose of this function is:
    - identify which dataframes need filtering
    - apply filtering rules based on another (already filtered if needed) dataframe.
    - (re)create the csv file related
    Args:
        files_data_dict : The list of objects which might be filtered
        df_paths (dataframe) : The paths dataframe - to know the filtering rules 
        filtering_category (str) : the category of files on df_paths which will be filtered
    Returns:
        a data dictionary with all objects filtered
    Raises:
        Exits the program if error running the function (using decorator)
    '''

    logging.info(f"FILES CATEGORY {filtering_category} -> FILTERING DATA [START]")

    #we get all files which can be filtered
    df_files_filter = df_paths[df_paths['FILTERING_CATEGORY'] == filtering_category].reset_index(drop=True)
    
    #some files reduce their scope using other files already scope reduced
    #we sort df_files_filter such as the one which use another file to be filtered (column FILTERING_FILE) is always sorted later
    def sort_dependency_relationships(df_files_filter):
        # Build a mapping from file names to their row indices
        files_to_index = {val: idx for idx, val in df_files_filter['NAME'].items()}

        # Create a directed graph
        G = nx.DiGraph()

        # Add all indices as nodes
        G.add_nodes_from(df_files_filter.index)

        # Add edges from the row where file name == FILTERING_FILE to the row where FILTERING_FILE == file_name
        for idx, b_val in df_files_filter['FILTERING_FILE'].items():
            if b_val in files_to_index:
                G.add_edge(files_to_index[b_val], idx)

        # Topological sort
        sorted_indices = list(nx.topological_sort(G))

        # Reorder the DataFrame
        df_filtered_sorted = df_files_filter.loc[sorted_indices].reset_index(drop=True)
        return df_filtered_sorted

    df_files_filter = sort_dependency_relationships(df_files_filter)
    
    for row in df_files_filter.itertuples(index=False):
        df_key = f'df_{row.NAME}'
        #for each existing file having a filtering rule
        if df_key in files_data_dict and row.FILTERING_COLUMN and row.FILTERING_FILE:
            df_to_filter = files_data_dict[df_key]
            df_filtering = files_data_dict[f'df_{row.FILTERING_FILE}'][row.FILTERING_COLUMN].drop_duplicates()
            #we merge with the filtering dataframe
            files_data_dict[df_key] = df_to_filter.merge(df_filtering, 
                                        on= row.FILTERING_COLUMN, 
                                        how='inner')

            #then we (re)create the csv corresponding to the filtered dataframe
            local_file_path = os.path.join(var.TMPF,row.NAME+'.csv')
            create_csv(local_file_path,files_data_dict[df_key],row.IS_FOR_UPLOAD)
            logging.info(f"FILE {row.NAME} -> FILTERED")

    logging.info(f"FILES CATEGORY {filtering_category} -> FILTERING DATA [DONE]")
    return files_data_dict
