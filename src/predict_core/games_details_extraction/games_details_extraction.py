''' 
    The purpose of this module is to interact with game website by:
    - getting the scope of game we need to extract 
    - then extract the games, and their details
'''

import logging
import os
import pandas as pd

from ..config import config_decorators
from ..config.config_variables import config_global_variables as var
from ..files_manipulation.local_files_manipulation.files_manipulation import create_csv
from .games_details_extraction_lnb.games_details_extraction_lnb import get_game_details_lnb

logging.basicConfig(level=logging.INFO)
game_info_functions = {
    "LNB": get_game_details_lnb
}

@config_decorators.exit_program(log_filter=lambda args: {k: args[k] for k in ('df_competition',)})
def extract_games_from_competition(df_competition: pd.DataFrame) -> pd.DataFrame:
    
    """
        Gets all games from list of competition while called by entry point function competition_integration
        Args:
            df_competition (dataframe): the list of competition from input files
        Returns:
            dataframe: contains all games (with their competition ids an season ids)     
        Raises:
            Exits the program if error running the function (using decorator)
    """
    logging.info("GAME -> GETTING GAMES [START]")
    
    df_game = pd.DataFrame(columns=['COMPETITION_SOURCE', 'COMPETITION_ID', 'COMPETITION_SOURCE_ID', 'SEASON_ID', 
               'GAMEDAY', 'DATE_GAME_UTC', 'TIME_GAME_UTC', 'DATE_GAME_LOCAL', 'TIME_GAME_LOCAL',
               'TEAM_HOME', 'SCORE_HOME', 'TEAM_AWAY', 'SCORE_AWAY', 'GAME_SOURCE_ID'])
    
    for compet_row in df_competition.itertuples(index=False):
        print(compet_row)
        source = compet_row.COMPETITION_SOURCE
        get_game_details = game_info_functions.get(source)
        df_game_details = get_game_details(competition_source_id = compet_row.COMPETITION_SOURCE_ID)
        df_game_details['COMPETITION_SOURCE'] = compet_row.COMPETITION_SOURCE
        df_game_details['COMPETITION_ID'] = compet_row.COMPETITION_ID
        df_game_details['SEASON_ID'] = compet_row.SEASON_ID
        df_game = pd.concat([df_game, df_game_details], ignore_index=True)
        logging.info(f"GAME -> COMPETITION {compet_row.COMPETITION_SOURCE} - {compet_row.COMPETITION_SOURCE_ID} extracted")

    create_csv(os.path.join(var.TMPF,'game.csv'),df_game,var.GAME_ENCAPSULATED) 

    logging.info("GAME -> GETTING GAMES [END]")
    return df_game

@config_decorators.exit_program(log_filter=lambda args: {})
def extract_games_from_need(sr_output_need: pd.Series,df_competition: pd.DataFrame, df_gameday_modification: pd.DataFrame) -> pd.DataFrame:
    
    """
        Gets games while called by entry point function main based on needs competition and gameday
        Args:
            sr_output_need (series - one row): the output_need we process - we extract only games from its gameday
            df_competition (DataFrame): the list of competition from input files, to get the source of game
            df_gameday_modification (DataFrame): the list of gameday modified from input files, to get the original name of gamedays
        Returns:
            dataframe: contains all games related with need competition and gameday   
        Raises:
            Exits the program if error running the function (using decorator)
    """
    
    logging.info("GAME -> GETTING GAMES [START]")
    
    competition_source = df_competition[(
         (df_competition['SEASON_ID'] == sr_output_need['SEASON_ID']) &
         (df_competition['COMPETITION_SOURCE_ID'] == sr_output_need['COMPETITION_SOURCE_ID']))].iloc[0]['COMPETITION_SOURCE']

    # If there are values in gameday_modification corresponding to the gameday needed
    # we filter on the original gameday from the source to get specific gamekeys wee need to extract
    sr_games_to_extract = df_gameday_modification[
        (df_gameday_modification["SEASON_ID"] == sr_output_need['SEASON_ID']) &
        (df_gameday_modification["GAMEDAY_MODIFIED"] == sr_output_need['GAMEDAY'])]['GAME_SOURCE_ID']

    # If there is no gameday modified, we keep the gameday from need
    get_game_details = game_info_functions.get(competition_source)
    if len(sr_games_to_extract) == 0:
        df_game = get_game_details(competition_source_id = sr_output_need['COMPETITION_SOURCE_ID'],
                                   gameday = sr_output_need['GAMEDAY'])
    else:
        df_game = get_game_details(competition_source_id = sr_output_need['COMPETITION_SOURCE_ID'],
                                   sr_games_to_extract = sr_games_to_extract)
        
    df_game['COMPETITION_SOURCE'] = competition_source
    df_game['COMPETITION_ID'] = sr_output_need['COMPETITION_ID']
    df_game['SEASON_ID'] = sr_output_need['SEASON_ID']
    columns = ['COMPETITION_SOURCE', 'COMPETITION_ID', 'COMPETITION_SOURCE_ID', 'SEASON_ID', 
               'GAMEDAY', 'DATE_GAME_UTC', 'TIME_GAME_UTC', 'DATE_GAME_LOCAL', 'TIME_GAME_LOCAL',
               'TEAM_HOME', 'SCORE_HOME', 'TEAM_AWAY', 'SCORE_AWAY', 'GAME_SOURCE_ID']
    
    df_game = df_game[columns].reset_index(drop=True)
    create_csv(os.path.join(var.TMPF,'game.csv'),df_game,var.GAME_ENCAPSULATED) 

    logging.info("GAME -> GETTING GAMES [END]")
    return df_game
