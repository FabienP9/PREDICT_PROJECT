''' 
    The purpose of this module is to interact with game website by:
    - getting the scope of game we need to extract 
    - then extract the games, and their details
'''

import logging
logging.basicConfig(level=logging.INFO)
import pandas as pd
import os

import config
from file_actions.file_actions import create_csv
from .game_actions_lnb.get_game_details_lnb import get_game_details_lnb

game_info_functions = {
    "LNB": get_game_details_lnb
}

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('df_competition',)})
def extract_games_from_competition(df_competition: pd.DataFrame) -> pd.DataFrame:
    
    """
        Gets all games from list of competition while called by exe_init_competition
        Args:
            df_competition (dataframe): the list of competition from input files
        Returns:
            dataframe: contains all games (with their competition ids an season ids)     
        Raises:
            Exits the program if error running the function (using decorator)
    """
    logging.info(f"GAME -> GETTING GAMES [START]")
    
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
        
    create_csv(os.path.join(config.TMPF,'game.csv'),df_game,config.game_encapsulated) 
    logging.info(f"GAME -> GETTING GAMES [END]")
    return df_game

@config.exit_program(log_filter=lambda args: {})
def extract_games_from_need(sr_output_need: pd.Series,df_competition: pd.DataFrame) -> pd.DataFrame:
    
    """
        Gets games while called by exe_main based on needs competition and gameday
        Args:
            sr_output_need (series - one row): the output_need we process - we extract only games from its gameday
            df_competition (DataFrame): the list of competition from input files, to get the filter competition
        Returns:
            dataframe: contains all games related with need competition and gameday   
        Raises:
            Exits the program if error running the function (using decorator)
    """
    
    logging.info(f"GAME -> GETTING GAMES [START]")
    
    compet_row = next(df_competition[
        (df_competition['SEASON_ID'] == sr_output_need['SEASON_ID']) &
        (df_competition['COMPETITION_ID'] == sr_output_need['COMPETITION_ID'])
    ].itertuples(index=False))
    
    source = compet_row.COMPETITION_SOURCE
    get_game_details = game_info_functions.get(source)
    df_game = get_game_details(compet_row,sr_output_need['GAMEDAY'])

    create_csv(os.path.join(config.TMPF,'game.csv'),df_game,config.game_encapsulated) 
    logging.info(f"GAME -> GETTING GAMES [END]")
    return df_game
