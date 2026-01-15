'''
    The purpose of this module is to generate message personalized for calculated gameday on forums topics.
    This module generates the calculated gameday message that users can copy to submit their own predictions.
    It gets the template message, and replace all parameters with calculated ones by connectiong to snowflake database
'''
import logging
logging.basicConfig(level=logging.INFO)
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.table import Table
import numpy as np
from typing import Tuple

from output_actions import output_actions as outputA
from output_actions import output_actions_sql_queries as sqlQ
from snowflake_actions import snowflake_execute
from imgbb_actions import push_capture_online
import config
from message_actions import post_message
import file_actions as fileA

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def get_calculated_games_result(sr_snowflake_account: pd.Series, sr_gameday_output_calculate: pd.Series) -> Tuple[str,int]:

    '''
        Gets the list of games to display on the output calculated message
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql game query
            sr_gameday_output_calculate (series - one row) containing the query filters
        Returns:
            A multiple row string displaying the list of games
            The number of games
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    #We call the games query by first personalizing it
    df_games = snowflake_execute(sr_snowflake_account,sqlQ.qGame,(sr_gameday_output_calculate['SEASON_ID'],sr_gameday_output_calculate['GAMEDAY']))
    
    #we add a + before the result if it is positive
    df_games['RESULT'] = df_games['RESULT'].map(lambda x: f"+{x}" if x > 0 else str(x))

    #we extract the list of games to display, concatenating fields into a string
    df_games['STRING'] = (df_games['GAME_MESSAGE_SHORT'].astype(str) +"/ " +
        df_games['TEAM_HOME_NAME'] + " vs " + 
        df_games['TEAM_AWAY_NAME']+ ": [b]" +
        df_games['RESULT'].astype(str) + "[/b] [ " +
        df_games['SCORE_HOME'].astype(str) +" - " + df_games['SCORE_AWAY'].astype(str)) + " ]"
    
    # we create the LIST_GAMES string by concatenating all games-strings on several lines
    RESULT_GAMES = "\n".join(df_games['STRING'])
    
    return RESULT_GAMES, len(df_games)

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

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def get_calculated_scores_detailed(sr_snowflake_account: pd.Series, sr_gameday_output_calculate: pd.Series) -> Tuple[pd.DataFrame,int]:

    '''
        Gets scores per users at predictions detail level
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql predict game query
            sr_gameday_output_calculate (series - one row) containing the query filters
        Returns:
            The dataframe of scores per user on a two header level for display
            The number of users concerned
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # We call the predictions games scores query 
    df_predict_games = snowflake_execute(sr_snowflake_account,sqlQ.qPredictGame,(sr_gameday_output_calculate['SEASON_ID'],sr_gameday_output_calculate['GAMEDAY']))
    # we remove useless column in this run context - we couldn't remove at the sources: see the related Snowflake view spec for more details
    df_predict_games = df_predict_games.dropna(axis=1, how='all')
    #if there are no columns remaining or no rows (no users) we just return like it
    if df_predict_games.shape[0] == 0 or df_predict_games.shape[1]  == 0:
        return df_predict_games,0
    else:
        # we remove GAMEDAY and SEASON_ID they were used to filter
        df_predict_games = df_predict_games.drop(columns=['GAMEDAY','SEASON_ID'])
        # We transform the dataframe on two level header
        split_cols = df_predict_games.columns.to_series().str.split('_', n=1, expand=True)

        # For 'NAME' and 'PT', set the second level to empty string
        split_cols[1] = split_cols[1].fillna('')  # fill None for columns without underscore
        split_cols.loc[split_cols[0].isin(['NAME', 'PT']), 1] = ''
 
        # Convert to MultiIndex
        df_predict_games.columns = pd.MultiIndex.from_frame(split_cols)
        # we replace NaN with empty string, and pandas generated floats columns (because of nans values) with int
        num_cols = df_predict_games.select_dtypes(include=['float', 'int']).columns
        df_predict_games[num_cols] = df_predict_games[num_cols].fillna(0).astype(int)
        df_predict_games = df_predict_games.fillna('')
        return df_predict_games, len(df_predict_games)

@config.exit_program(log_filter=lambda args: {'columns_df_userscores_global': args['df_userscores_global'].columns.tolist(),})
def get_calculated_scores_global(df_userscores_global: pd.DataFrame) -> Tuple[pd.DataFrame,int]:

    '''
        The purpose of this function is to:
        - get scores per user and season
        - rank users per score descending
        Inputs:
            df_userscores_global (dataframe) containing all scores
        Returns:
            The dataframe of scores, with ranked users
            The number of users       
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    df_userscores_modified = df_userscores_global.copy()
    df_userscores_modified = outputA.display_rank(df_userscores_modified,'RANK')
    df_userscores_modified = df_userscores_modified[['RANK','USER_NAME', 'TOTAL_POINTS', 'NB_GAMEDAY_PREDICT', 'NB_GAMEDAY_FIRST', 'NB_TOTAL_PREDICT']]
    return df_userscores_modified, len(df_userscores_modified)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def get_calculated_scores_gameday(sr_snowflake_account: pd.Series, sr_gameday_output_calculate: pd.Series) -> Tuple[pd.DataFrame,int]:

    '''
        The purpose of this function is to:
        - get scores and number of predictions per user for a specified gameday
        - rank user by scores descending
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql predict game query
            sr_gameday_output_calculate (series - one row) containing the query filters
        Returns:
            The dataframe of scores, with ranked users
            The number of users   
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    df_userscores_gameday = snowflake_execute(sr_snowflake_account,sqlQ.qUserScores_Gameday,(sr_gameday_output_calculate['SEASON_ID'],sr_gameday_output_calculate['GAMEDAY']))
    df_userscores_gameday = outputA.display_rank(df_userscores_gameday,'RANK')
    df_userscores_gameday = df_userscores_gameday[['RANK','USER_NAME', 'GAMEDAY_POINTS', 'NB_PREDICTION_GAMEDAY', 'AVERAGE_POINTS']]
    return df_userscores_gameday, len(df_userscores_gameday)

@config.exit_program(log_filter=lambda args: {'nb_prediction': args['nb_prediction'], 'columns_df_userscores_global': args['df_userscores_global'].columns.tolist() })
def get_calculated_scores_average(nb_prediction: int, df_userscores_global: pd.DataFrame) -> Tuple[str,int, int]:

    '''
        The purpose of this function is to:
        - get average scores per gameday per user per season
        - rank user per average score descending, only for users with more than half participation in number of prediction
        Inputs:
            nb_prediction (int) - it will be used to calculate the number min prediction
            df_userscores_global (dataframe) containing all average scores
        Returns:
            The string with user ranked by average scores 
            The number of users
            The number of minimal predictions done needed for the scope of users
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    df_userscores_modified = df_userscores_global.copy()
    # we calculate the min number of gameday to be part of this ranking
    NB_MIN_PREDICTION = int(nb_prediction/2)
    df_userscores_modified = df_userscores_modified[df_userscores_modified['NB_TOTAL_PREDICT'] > NB_MIN_PREDICTION]
    df_userscores_modified = outputA.calculate_and_display_rank(df_userscores_modified,['AVERAGE_POINTS'])
    df_userscores_modified['STRING'] = (df_userscores_modified['RANK'].astype(str) + ". " +
                                        df_userscores_modified['USER_NAME'] + " - " +
                                        df_userscores_modified['AVERAGE_POINTS'].astype(str) + " pts")
    # we create the SCORES_AVERAGE string by concatenating all users on several lines
    SCORES_AVERAGE = "\n".join(df_userscores_modified['STRING'])
    return SCORES_AVERAGE, len(df_userscores_modified),NB_MIN_PREDICTION

@config.exit_program(log_filter=lambda args: {'columns_df_gamepredictchamp': args['df_gamepredictchamp'].columns.tolist(), 'sr_gameday_output_calculate': args['sr_gameday_output_calculate'] })
def get_calculated_predictchamp_result(df_gamepredictchamp: pd.DataFrame, sr_snowflake_account: pd.Series, sr_gameday_output_calculate: pd.Series) -> str:

    '''
        Gets scores for prediction championship gamedays - per teams and per user of each teams
        Inputs:
            df_gamepredictchamp (df) scores per games and teams, with winner features
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql predictchamp game query
            sr_gameday_output_calculate (series - one row) containing the query filters
        Returns:
            The string displaying games result, per teams then detailed by user related
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    # we create the string for displaying result game according to winner
    df_gamepredictchamp_modified = df_gamepredictchamp.copy()
    df_gamepredictchamp_modified['STRING_TEAM_HOME'] = np.where(df_gamepredictchamp_modified['WINNER'] == 1,
    "[b]" + df_gamepredictchamp_modified['TEAM_HOME_NAME'] + "[/b]", df_gamepredictchamp_modified['TEAM_HOME_NAME'])
    
    df_gamepredictchamp_modified['STRING_TEAM_AWAY'] = np.where(df_gamepredictchamp_modified['WINNER'] == 2,
    "[b]" + df_gamepredictchamp_modified['TEAM_AWAY_NAME'] + "[/b]", df_gamepredictchamp_modified['TEAM_AWAY_NAME'])
    
    df_gamepredictchamp_modified['STRING'] = (df_gamepredictchamp_modified['GAME_MESSAGE_SHORT'].astype(str) + "/ " +
                                              df_gamepredictchamp_modified['STRING_TEAM_HOME'] + " vs " +
                                              df_gamepredictchamp_modified['STRING_TEAM_AWAY'] + " : " +
                                              df_gamepredictchamp_modified['POINTS_HOME'].astype(str) + " - " +
                                              df_gamepredictchamp_modified['POINTS_AWAY'].astype(str))

    # We call the predictions championship scores per user query, containing team related key
    df_gamepredictchamp_detail = snowflake_execute(sr_snowflake_account,sqlQ.qGamePredictchampDetail,(sr_gameday_output_calculate['SEASON_ID'],sr_gameday_output_calculate['GAMEDAY']))
    # we create the string of score for each user, depending on their rank within the team

    df_gamepredictchamp_detail['STRING'] = np.where(
                                            df_gamepredictchamp_detail['RANK_USER_TEAM'] == 1, 
                                            df_gamepredictchamp_detail['USER_NAME'] + ": " + df_gamepredictchamp_detail['POINTS'].astype(str) + " pts" , 
                                            df_gamepredictchamp_detail['USER_NAME'] + " (" + df_gamepredictchamp_detail['POINTS'].astype(str) + ')')

    # We aggregate leaders users and others per team
    def teams_group(g: pd.DataFrame) -> str:
        team_string = "-> " + "\n-> ".join(g.loc[g["RANK_USER_TEAM"] == 1, "STRING"])
        others = g.loc[g["RANK_USER_TEAM"] != 1, "STRING"].tolist()
        if others:
            team_string += "   - [__Not counted__: " + "/ ".join(others) + "]"
        return team_string

    df_team_summary = (
        df_gamepredictchamp_detail
        .groupby(["GAME_KEY", "TEAM_NAME"], as_index=False)
        .apply(lambda g: pd.Series({"TEAM_STRING": teams_group(g)}))
        .reset_index(drop=True)
    )

    # we add bonus for home teams
    home_bonus = df_gamepredictchamp_modified[["GAME_KEY", "TEAM_HOME_NAME", "POINTS_BONUS"]].rename(columns={"TEAM_HOME_NAME": "TEAM_NAME"})
    df_team_summary = df_team_summary.merge(home_bonus, on=["GAME_KEY", "TEAM_NAME"], how="left")
    bonus_str = df_team_summary["POINTS_BONUS"].where(df_team_summary["POINTS_BONUS"].notna(), 0)
    bonus_str = bonus_str.astype(float) # ensure float to allow safe fill
    df_team_summary["TEAM_STRING"] = np.where(
        bonus_str != 0,
        df_team_summary["TEAM_STRING"] + "\n-> __Home bonus__: " + bonus_str.astype(int).astype(str) + " pts",
        df_team_summary["TEAM_STRING"]
    )
    
    # We wrap in [code] block
    df_team_summary["TEAM_STRING"] = "[code]__FOR__ " + df_team_summary["TEAM_NAME"] + ":\n" + df_team_summary["TEAM_STRING"] + "[/code]"
    
    # We aggregate team string per game
    df_game_output = (
        df_team_summary
        .groupby("GAME_KEY", as_index=False)
        .agg(TEAM_STRING=("TEAM_STRING", "\n".join))
    )
    # we calculate the final string
    df_final = df_gamepredictchamp_modified.merge(df_game_output, on="GAME_KEY", how="left")
    df_final["RESULT_STRING"] = np.where(
        df_final["TEAM_STRING"].notna(),
        df_final["STRING"] + "\n" + df_final["TEAM_STRING"],
        df_final["STRING"]
    )
    RESULTS_PREDICTCHAMP = "\n".join(df_final["RESULT_STRING"].tolist())
    return RESULTS_PREDICTCHAMP

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def get_calculated_predictchamp_ranking(sr_snowflake_account: pd.Series, sr_gameday_output_calculate: pd.Series) -> pd.DataFrame:

    '''
        Gets the ranking of the prediction championship
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql teams query
            sr_gameday_output_calculate (series - one row) containing the query filters
        Returns:
            The dataframe displaying the ranking
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # We call the teams query
    df_teamscores = snowflake_execute(sr_snowflake_account,sqlQ.qTeamScores,(sr_gameday_output_calculate['SEASON_ID'],))

    #we add a rank per team - first by percentage of win, then by points difference
    df_teamscores = outputA.display_rank(df_teamscores,'RANK')
    
    return df_teamscores

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def get_calculated_correction(sr_snowflake_account: pd.Series, sr_gameday_output_calculate: pd.Series) -> Tuple[str,int]:

    '''
        Gets correction per user for a specific gameday
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql corrections query
            sr_gameday_output_calculate (series - one row) containing the query filters
        Returns:
            The string of the corrections -one user per line -  
            The number of user concerned
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # We call the correction query
    df_correction = snowflake_execute(sr_snowflake_account,sqlQ.qCorrection,(sr_gameday_output_calculate['SEASON_ID'],sr_gameday_output_calculate['GAMEDAY']))

    # we create the string of corrections group by user - one per line
    grouped = df_correction.groupby('USER_NAME')['PREDICT_ID'].apply(lambda x: f"{x.name} : {' / '.join(map(str, x))}")
    LIST_CORRECTION = "\n".join(grouped.tolist())

    return LIST_CORRECTION, grouped.shape[0]

@config.exit_program(log_filter=lambda args: {'columns_df_gameday_calculated': args['df_gameday_calculated'].columns.tolist(),})
def get_calculated_list_gameday(df_gameday_calculated: pd.DataFrame) -> str:

    '''
        Gets the list of gameday calculated to display as a string on the output calculated message
        Inputs:
            df_gameday_calculated (dataframe) containing list of calculated gameday
        Returns:
            A multiple row string displaying the list of gameday
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    df_gameday_calculated_modified = df_gameday_calculated.copy()
    df_gameday_calculated_modified['STRING'] = (df_gameday_calculated['GAMEDAY'] + " (" + df_gameday_calculated['NB_PREDICTION'].astype(str) + ")")
    LIST_GAMEDAY_CALCULATED = " / ".join(df_gameday_calculated_modified['STRING'])

    return LIST_GAMEDAY_CALCULATED

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def get_mvp_month_race_figure(sr_snowflake_account: pd.Series, sr_gameday_output_calculate: pd.Series) -> Tuple[str, str, int]:

    '''
        Gets figures for monthly MVP election
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql mvp query
            sr_gameday_output_calculate (series - one row) containing the query filters
        Returns:
            The month of the gameday
            The string of the corrections -one user per line -  
            The number of user concerned
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    GAMEDAY_MONTH = sr_gameday_output_calculate['END_MONTH_LOCAL']
    # We call the MVPRace Month query
    df_month_mvp = snowflake_execute(sr_snowflake_account,sqlQ.qMVPRace_month_figures,(sr_gameday_output_calculate['SEASON_ID'],sr_gameday_output_calculate['END_YEARMONTH_LOCAL']))

    #we create the output string - without the team display if there are not
    df_month_mvp['STRING'] = np.where(df_month_mvp['LIST_TEAMS'] == '',
                                  df_month_mvp['USER_NAME'] + " - " + df_month_mvp['POINTS'].astype(str) + " pts",
                                  df_month_mvp['USER_NAME'] + " - " + df_month_mvp['POINTS'].astype(str) + " pts / " + df_month_mvp['WIN'].astype(str) + "__W__-" + df_month_mvp['LOSS'].astype(str) + "__L__ [__with__ " + df_month_mvp['LIST_TEAMS'].astype(str) + "]")
    LIST_USER_MONTH = "\n".join(df_month_mvp['STRING'].tolist())
    return GAMEDAY_MONTH, LIST_USER_MONTH, len(df_month_mvp)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def get_mvp_compet_race_figure(sr_snowflake_account: pd.Series, sr_gameday_output_calculate: pd.Series) -> Tuple[str, str, int]:

    '''
        Gets figures for competition MVP election
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run the sql mvp query
            sr_gameday_output_calculate (series - one row) containing the query filters
        Returns:
            The competition
            The string of the corrections -one user per line -  
            The number of user concerned
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    GAMEDAY_COMPETITION = sr_gameday_output_calculate['COMPETITION_LABEL']
    # We call the MVPRace competition query
    df_compet_mvp = snowflake_execute(sr_snowflake_account,sqlQ.qMVPRace_Compet_figures,(sr_gameday_output_calculate['SEASON_ID'],GAMEDAY_COMPETITION))

    #we create the output string - without the team display if there are not
    df_compet_mvp['STRING'] = np.where(df_compet_mvp['LIST_TEAMS'] == '',
                                  df_compet_mvp['USER_NAME'] + " - " + df_compet_mvp['POINTS'].astype(str) + " pts",
                                  df_compet_mvp['USER_NAME'] + " - " + df_compet_mvp['POINTS'].astype(str) + " pts / " + df_compet_mvp['WIN'].astype(str) + "__W__-" + df_compet_mvp['LOSS'].astype(str) + "__L__ [__with__ " + df_compet_mvp['LIST_TEAMS'].astype(str) + "]")

    LIST_USER_COMPETITION = "\n".join(df_compet_mvp['STRING'].tolist())
    return GAMEDAY_COMPETITION, LIST_USER_COMPETITION, len(df_compet_mvp)

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def get_calculated_parameters(sr_snowflake_account: pd.Series, sr_gameday_output_calculate: pd.Series) -> dict:

    '''
        Defines all parameters for calculated message
        Inputs:
            sr_snowflake_account (series - one row) containing snowflake credentials to run queries
            sr_gameday_output_calculate (series - one row) containing parameters to filter queries
        Returns:
            data dictionary with all calculated parameters
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    param_dict= {}
    param_dict['GAMEDAY'] = sr_gameday_output_calculate['GAMEDAY']
    param_dict['SEASON_DIVISION'] = sr_gameday_output_calculate['SEASON_DIVISION']
    param_dict['RESULT_GAMES'],param_dict['NB_GAMES'] = get_calculated_games_result(sr_snowflake_account,sr_gameday_output_calculate)
    param_dict['SCORES_DETAILED_DF'], param_dict['NB_USER_DETAIL'] = get_calculated_scores_detailed(sr_snowflake_account,sr_gameday_output_calculate) 

    # we get the scores per users query 
    df_userscores_global = snowflake_execute(sr_snowflake_account,sqlQ.qUserScores_Global,(sr_gameday_output_calculate['SEASON_ID'],))
    param_dict['SCORES_GLOBAL_DF'],param_dict['NB_USER_GLOBAL'] = get_calculated_scores_global(df_userscores_global)

    # we get the gamedays calculated query 
    df_gameday_calculated = snowflake_execute(sr_snowflake_account,sqlQ.qList_Gameday_Calculated,(sr_gameday_output_calculate['SEASON_ID'],))
    param_dict['NB_GAMEDAY_CALCULATED'] = len(df_gameday_calculated)
    param_dict['NB_TOTAL_PREDICT'] = df_gameday_calculated['NB_PREDICTION'].sum()
    param_dict['NB_MAX_PREDICT'] = df_gameday_calculated.loc[df_gameday_calculated['GAMEDAY'] == param_dict['GAMEDAY'], 'NB_PREDICTION'].iloc[0]
    param_dict['SCORES_AVERAGE'] ,param_dict['NB_USER_AVERAGE'], param_dict['NB_MIN_PREDICTION'] = get_calculated_scores_average(param_dict['NB_TOTAL_PREDICT'],df_userscores_global)

    param_dict['SCORES_GAMEDAY_DF'],param_dict['NB_USER_GAMEDAY'] = get_calculated_scores_gameday(sr_snowflake_account,sr_gameday_output_calculate)
    param_dict['LIST_GAMEDAY_CALCULATED'] =  get_calculated_list_gameday(df_gameday_calculated)

    # we get the prediction championship results query
    df_gamepredictchamp = snowflake_execute(sr_snowflake_account,sqlQ.qGamePredictchamp,(sr_gameday_output_calculate['SEASON_ID'],sr_gameday_output_calculate['GAMEDAY']))
    param_dict['NB_GAME_PREDICTCHAMP'] = len(df_gamepredictchamp)

    #if there is no prediction championship games, we don't display the results
    if len(df_gamepredictchamp) == 0:
        param_dict['IS_FOR_RANK'] = 0
        param_dict['RESULTS_PREDICTCHAMP'] = None
        param_dict['HAS_HOME_ADV'] = 0
    else:
        param_dict['IS_FOR_RANK'] = df_gamepredictchamp.at[0,'IS_FOR_RANK']
        param_dict['RESULTS_PREDICTCHAMP'] = get_calculated_predictchamp_result(df_gamepredictchamp,sr_snowflake_account,sr_gameday_output_calculate)
        param_dict['HAS_HOME_ADV'] = df_gamepredictchamp.at[0,'HAS_HOME_ADV']

    # if the predictions games are not for rank, we don't display the predictions championship ranking
    if param_dict['IS_FOR_RANK'] == 0:
        param_dict['RANK_PREDICTCHAMP_DF'] = None
    else:
        param_dict['RANK_PREDICTCHAMP_DF'] = get_calculated_predictchamp_ranking(sr_snowflake_account,sr_gameday_output_calculate)

    param_dict['LIST_CORRECTION'],param_dict['NB_CORRECTION'] = get_calculated_correction(sr_snowflake_account,sr_gameday_output_calculate)

    # we process MVP race figures, only if need to display them
    if sr_gameday_output_calculate['DISPLAY_MONTH_MVP_RANKING']  == 1:
        GAMEDAY_MONTH, LIST_USER_MONTH, NB_USER_MONTH = get_mvp_month_race_figure(sr_snowflake_account,sr_gameday_output_calculate)
        param_dict['GAMEDAY_MONTH'] = GAMEDAY_MONTH
        param_dict['LIST_USER_MONTH'] = LIST_USER_MONTH
        param_dict['NB_USER_MONTH'] = NB_USER_MONTH
    else:
        param_dict['GAMEDAY_MONTH'] = None
        param_dict['LIST_USER_MONTH'] = None
        param_dict['NB_USER_MONTH'] = 0

    if sr_gameday_output_calculate['DISPLAY_COMPET_MVP_RANKING']  == 1:
        GAMEDAY_COMPETITION, LIST_USER_COMPETITION, NB_USER_COMPETITION = get_mvp_compet_race_figure(sr_snowflake_account,sr_gameday_output_calculate)
        param_dict['GAMEDAY_COMPETITION'] = GAMEDAY_COMPETITION
        param_dict['LIST_USER_COMPETITION'] = LIST_USER_COMPETITION
        param_dict['NB_USER_COMPETITION'] = NB_USER_COMPETITION
    else:
        param_dict['GAMEDAY_COMPETITION'] = None
        param_dict['LIST_USER_COMPETITION'] = None
        param_dict['NB_USER_COMPETITION'] = 0

    return param_dict

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate','country')})
def derive_calculated_parameters_for_country(param_dict: dict, sr_gameday_output_calculate: pd.Series, country: str,translations: list[str]) -> dict:

    '''
        Calculates derived parameters from a part of the the one retrieved by:
        - translating parameters for the given country
        - capturing specific translated dataframe parameters in jpg parameter
        - push captures on imgbb online provider and get the url parameter
        Inputs:
            param_dict (data dictionary) contains calculated parameters we want to derive for the given country
            sr_gameday_output_calculate (serie - one row): used to calculate derived parameters
            country (str): we translate parameters for this country
            translations (list): contains all strings to translate
        Returns:
            translated, captured, and url parameters into a dictionary
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    translated_dict={}
    captured_dict={}

    #For each parameter we translate
    for (key,value) in param_dict.items():
        translated_dict[key+'_'+country] = outputA.translate_param_for_country(value,country,translations)
    
    # we define capture behavior for specific parameters
    capture_configs = {
        "SCORES_DETAILED_DF": {
            "capture_func": capture_scores_detailed,
            "filename_prefix": "table_score_details"
        },
        "SCORES_GLOBAL_DF": {
            "capture_func": outputA.capture_df_oneheader,
            "filename_prefix": "table_global_scores"
        },
        "SCORES_GAMEDAY_DF": {
            "capture_func": outputA.capture_df_oneheader,
            "filename_prefix": "table_gameday_scores"
        },
        "RANK_PREDICTCHAMP_DF": {
            "capture_func": outputA.capture_df_oneheader,
            "filename_prefix": "table_predictchamp_ranking"
        }
    }

    for key, captconfig in capture_configs.items():
        translated_key = key+'_'+country
        if translated_key in translated_dict:
            filename = outputA.define_filename(captconfig["filename_prefix"], sr_gameday_output_calculate, 'jpg', country)
            local_path = os.path.join(config.TMPF, filename)

            # Capture and push image
            captconfig["capture_func"](translated_dict[translated_key], filename)
            url = push_capture_online(local_path)

            # Store metadata
            captured_dict[f"{key}_CAPTURE_{country}"] = filename
            captured_dict[f"{key}_URL_{country}"] = url

    return translated_dict | captured_dict

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def derive_calculated_parameters(param_dict: dict, sr_gameday_output_calculate: pd.Series, list_countries: list[str]) -> dict:

    '''
        Calls derive_calculated_parameters_for_country for each country, parallelizing it
        Inputs:
            param_dict (data dictionary) contains calculated parameters - we derive part of them
            sr_gameday_output_calculate (serie - one row): used to calculate derived parameters
            list_countries (list): we call derive_calculated_parameters_for_country for each country of the list
        Returns:
            dict with all parameters derived
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # we get the json file of translation
    translations = fileA.read_json("output_actions/output_actions_translations.json")

    # we filter only the relevant non-None entries and non empty dataframe from param_dict
    keys_to_check = ['SCORES_DETAILED_DF', 'SCORES_GLOBAL_DF', 'SCORES_GAMEDAY_DF', 'RANK_PREDICTCHAMP_DF','RESULTS_PREDICTCHAMP','GAMEDAY_MONTH','LIST_USER_MONTH','GAMEDAY_COMPETITION','LIST_USER_COMPETITION']
    dict_to_derive = {}
    for key in keys_to_check:
        value = param_dict.get(key)
        if value is None:
            continue
        if isinstance(value, pd.DataFrame) and value.empty:
            continue
        dict_to_derive[key] = value
    
    #we then call derive_calculated_parameters_for_country, for each country parallelizing
    param_dict_derived = {}
    param_args= [(dict_to_derive,sr_gameday_output_calculate,country,translations) for country in list_countries]
    results = config.multithreading_run(derive_calculated_parameters_for_country, param_args)
    param_dict_derived.update({k: v for r in results for k, v in r.items()})
    return param_dict_derived

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate','country')})
def create_calculated_messages_for_country(param_dict: dict, country: str, template:str, sr_gameday_output_calculate: pd.Series) -> Tuple[str,str]:

    '''
        Defines calculated message for each given country:
        - by replacing text with calculated parameters
        - removing blocks of text according to the value of some of the calculated parameters
        - create the text file containing the message
        Inputs:
            param_dict (data dictionary) containing parameters
            template (str): the message text we want to personalize
            country (str): the country of the forum for the message, some parameters depend on it
        Returns:
            the message personalized with its file name
            the country concerned with the message
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    # we replace |N| in the text with N*newlines
    content = outputA.format_message(template)

    # we replace parameters...
    # list is (replacement_field, replacement_value)
    replacement_substr = [
        ("#MESSAGE_PREFIX_PROGRAM_STRING#", config.message_prefix_program_string),
        ("#GAMEDAY#",param_dict['GAMEDAY']),
        ("#SEASON_DIVISION#",param_dict['SEASON_DIVISION']),
        ("#RESULT_GAMES#",param_dict['RESULT_GAMES']),
        ("#NB_GAMEDAY_CALCULATED#",str(param_dict['NB_GAMEDAY_CALCULATED'])),
        ("#NB_MAX_PREDICT#",str(param_dict['NB_MAX_PREDICT'])),
        ("#NB_TOTAL_PREDICT#",str(param_dict['NB_TOTAL_PREDICT'])),
        ("#LIST_GAMEDAY_CALCULATED#",param_dict['LIST_GAMEDAY_CALCULATED']) 
    ]

    if param_dict['NB_USER_DETAIL'] > 0:   
        replacement_substr.extend([
            ("#IMGGAMEDAY#",param_dict['SCORES_GAMEDAY_DF_URL_'+country]),
            ("#IMGDETAIL#",param_dict['SCORES_DETAILED_DF_URL_'+country]),
            ("#NB_GAMES#",str(param_dict['NB_GAMES']))
        ])

    if param_dict['NB_CORRECTION'] > 0:
        replacement_substr.extend([
            ("#LIST_USER_SCOREAUTO0#",param_dict['LIST_CORRECTION'])
        ])

    if param_dict['NB_USER_GLOBAL'] > 0:
        replacement_substr.extend([
            ("#IMGSEASON#",param_dict['SCORES_GLOBAL_DF_URL_'+country])
        ])

    if param_dict['NB_USER_AVERAGE'] > 0:
        replacement_substr.extend([
            ("#NB_MIN_PREDICTION#",str(param_dict['NB_MIN_PREDICTION'])),
            ("#SCORES_AVERAGE#",param_dict['SCORES_AVERAGE'])
        ])    

    if param_dict['NB_GAME_PREDICTCHAMP'] > 0:
        replacement_substr.extend([
            ("#RESULTS_PREDICTCHAMP#",param_dict['RESULTS_PREDICTCHAMP_'+country])
        ])  

    if param_dict['IS_FOR_RANK'] == 1:
        replacement_substr.extend([
            ("#RANK_PREDICTCHAMP_IMG#",param_dict['RANK_PREDICTCHAMP_DF_URL_'+country])
        ])  

    if param_dict['NB_USER_MONTH'] > 0:
        replacement_substr.extend([
            ("#GAMEDAY_MONTH#",param_dict['GAMEDAY_MONTH_'+country]),
            ("#LIST_USER_MONTH#",param_dict['LIST_USER_MONTH_'+country]),
            ("#NB_USER_MONTH#",str(param_dict['NB_USER_MONTH']))
        ])  

    if param_dict['NB_USER_COMPETITION'] > 0:
        replacement_substr.extend([
            ("#GAMEDAY_COMPETITION#",param_dict['GAMEDAY_COMPETITION_'+country]),
            ("#LIST_USER_COMPETITION#",param_dict['LIST_USER_COMPETITION_'+country]),
            ("#NB_USER_COMPETITION#",str(param_dict['NB_USER_COMPETITION']))
        ])          

    for replacement_field, replacement_value in replacement_substr:
        content = content.replace(replacement_field,replacement_value) 

    # ... and possibly block if condition not True
    conditional_blocks = [ #begin_tag , end_tag, condition
        ("#WITH_PREDICTORS_GAMEDAY_BEGIN#", "#WITH_PREDICTORS_GAMEDAY_END#", param_dict['NB_USER_DETAIL'] > 0),
        ("#WITHOUT_PREDICTORS_GAMEDAY_BEGIN#", "#WITHOUT_PREDICTORS_GAMEDAY_END#", param_dict['NB_USER_DETAIL'] == 0),
        ("#SCOREAUTO0_BEGIN#", "#SCOREAUTO0_END#", param_dict['NB_CORRECTION'] > 0),
        ("#WITH_PREDICTORS_GLOBAL_BEGIN#", "#WITH_PREDICTORS_GLOBAL_END#", param_dict['NB_USER_GLOBAL'] > 0),
        ("#WITHOUT_PREDICTORS_GLOBAL_BEGIN#", "#WITHOUT_PREDICTORS_GLOBAL_END#", param_dict['NB_USER_GLOBAL'] == 0),
        ("#WITH_PREDICTORS_AVERAGE_BEGIN#", "#WITH_PREDICTORS_AVERAGE_END#", param_dict['NB_USER_AVERAGE'] > 0),
        ("#WITHOUT_PREDICTORS_AVERAGE_BEGIN#", "#WITHOUT_PREDICTORS_AVERAGE_END#", param_dict['NB_USER_AVERAGE'] == 0),
        ("#WITH_PREDICTCHAMP_BEGIN#", "#WITH_PREDICTCHAMP_END#", param_dict['NB_GAME_PREDICTCHAMP'] > 0),
        ("#WITHOUT_PREDICTCHAMP_BEGIN#", "#WITHOUT_PREDICTCHAMP_END#", param_dict['NB_GAME_PREDICTCHAMP'] == 0),
        ("#WITH_HOME_ADV_BEGIN#", "#WITH_HOME_ADV_END#", param_dict['HAS_HOME_ADV'] == 1),
        ("#WITHOUT_HOME_ADV_BEGIN#", "#WITHOUT_HOME_ADV_END#", param_dict['HAS_HOME_ADV'] == 0),
        ("#WITH_PREDICTCHAMPRANKING_BEGIN#", "#WITH_PREDICTCHAMPRANKING_END#", param_dict['IS_FOR_RANK'] == 1),
        ("#WITH_MONTH_MVP_BEGIN#", "#WITH_MONTH_MVP_END#", param_dict['NB_USER_MONTH'] > 0),
        ("#WITH_COMPETITION_MVP_BEGIN#", "#WITH_COMPETITION_MVP_END#", param_dict['NB_USER_COMPETITION'] > 0)
    ]

    for begin_tag, end_tag, condition in conditional_blocks:
        content = outputA.replace_conditionally_message(content,begin_tag, end_tag, condition)

    file_name = outputA.define_filename("forumoutput_calculated", sr_gameday_output_calculate, 'txt', country)
    fileA.create_txt(os.path.join(config.TMPF,file_name),content)
    return content,country

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('sr_gameday_output_calculate',)})
def process_output_message_calculated(context_dict: dict, sr_gameday_output_calculate: pd.Series):

    '''
        Defines calculated gameday message for each topics and country we want to post:
        - by getting templates
        - modify templates with parameters calculated 
        - posting the text on forums
        Inputs:
            context_dict (data dictionary) containing data to calculate the parameters
            sr_gameday_output_calculate (series - one row) containing details to calculate the parameters
        Raises:
            Exits the program if error running the function (using decorator)
    '''

    logging.info(f"OUTPUT -> GENERATING CALCULATED MESSAGE [START]")

    sr_snowflake_account = context_dict['sr_snowflake_account_connect']
    # we get the distinct list of topics where we want to post, and the list of distinct countries for these topics
    df_topics = snowflake_execute(sr_snowflake_account,sqlQ.qTopics_Calculate,(sr_gameday_output_calculate['SEASON_ID'],))
    list_countries = df_topics['FORUM_COUNTRY'].unique().tolist()

    param_dict_retrieve = get_calculated_parameters(sr_snowflake_account,sr_gameday_output_calculate)
    logging.info(f"OUTPUT -> PARAM RETRIEVED")
    param_dict_derived = derive_calculated_parameters(param_dict_retrieve,sr_gameday_output_calculate,list_countries)
    logging.info(f"OUTPUT -> PARAM DERIVED")
    param_dict = param_dict_retrieve | param_dict_derived

    message_args= [(param_dict,country,context_dict['str_output_gameday_calculation_template_'+country],sr_gameday_output_calculate) for country in list_countries]
    results = config.multithreading_run(create_calculated_messages_for_country, message_args)
    for content,country in results:
        param_dict['MESSAGE_'+country] = content
    logging.info(f"OUTPUT -> MESSAGES CALCULATED")

    posting_args = [(row,param_dict['MESSAGE_'+row['FORUM_COUNTRY']]) for _,row in df_topics.iterrows()]
    config.multithreading_run(post_message, posting_args)
    logging.info(f"OUTPUT -> GENERATING CALCULATED MESSAGE [DONE]")