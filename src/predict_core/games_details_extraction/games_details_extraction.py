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
    
    df_game = pd.DataFrame(columns=['COMPETITION_SOURCE','COMPETITION_ID','SEASON_ID',
                                    'GAMEDAY','DATE_GAME_UTC', 'TIME_GAME_UTC','DATE_GAME_LOCAL',
                                    'TIME_GAME_LOCAL','TEAM_HOME','SCORE_HOME',
                                    'TEAM_AWAY','SCORE_AWAY','GAME_SOURCE_ID'])
    
    for compet_row in df_competition.itertuples(index=False):
        source = compet_row.COMPETITION_SOURCE
        get_game_details = game_info_functions.get(source)
        df_game_details = get_game_details(compet_row)
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
            df_competition (DataFrame): the list of competition from input files, to get the filter competition
        Returns:
            dataframe: contains all games related with need competition and gameday   
        Raises:
            Exits the program if error running the function (using decorator)
    """
    
    logging.info("GAME -> GETTING GAMES [START]")
    
    compet_row = next(df_competition[
        (df_competition['SEASON_ID'] == sr_output_need['SEASON_ID']) &
        (df_competition['COMPETITION_ID'] == sr_output_need['COMPETITION_ID'])
    ].itertuples(index=False))
    
    source = compet_row.COMPETITION_SOURCE
    get_game_details = game_info_functions.get(source)
    df_game = get_game_details(compet_row,sr_output_need['GAMEDAY'],df_gameday_modification)

    create_csv(os.path.join(var.TMPF,'game.csv'),df_game,var.GAME_ENCAPSULATED) 
    logging.info("GAME -> GETTING GAMES [END]")
    return df_game
