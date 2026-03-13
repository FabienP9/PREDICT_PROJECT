'''
    The purpose of this module is to extract game details coming from the LNB website 
''' 

import requests
import pandas as pd
from zoneinfo import ZoneInfo
import os

import config

@config.exit_program(log_filter=lambda args: dict(args))
@config.retry_function(log_filter=lambda args: dict(args))
def get_game_details_lnb(competition_row: tuple, gameday: str | None = None) -> pd.DataFrame:

    """
        Gets all games details from a competition coming from LNB website, managed in JSON, possibly filtered by gameday
        Args:
            competition_row (tuple) : Basic details about the competition we want to extract from LNB website
            gameday (str): Gameday to filter on if exists
        Returns:
            the dataframe corresponding to all games details extracted from this competition and possibly gameday
        Raises:
            Retry 3 times and exits the program if error with extraction or parsing (using retry decorator)
    """

    url = "https://api-prod.lnb.fr/match/getCalendar"
    payload = {
        "competition_external_id": int(competition_row.COMPETITION_SOURCE_ID),
        "start_date": "2000-01-01",
        "end_date": "2999-12-31"
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    data = response.json().get("data", [])
    df_game = pd.json_normalize(data, record_path="data", errors="ignore")

    if gameday is not None:
        df_game = df_game[df_game["round_description"].astype(str) == gameday]

    game_status = df_game["match_status"]
    if not game_status.isin(['SCHEDULED','COMPLETE']).all() and os.getenv('OVERWRITE_GAMES_STATUS') == 0:
        raise ValueError("At least one game is in progress or unknow status- retry extraction later")
    
    df_game["COMPETITION_SOURCE"] = competition_row.COMPETITION_SOURCE
    df_game["COMPETITION_ID"] = competition_row.COMPETITION_ID
    df_game["SEASON_ID"] = competition_row.SEASON_ID
    df_game["GAMEDAY"] = df_game["round_description"]

    # we get datetime of game
    df_game["DATETIME_UTC"] = pd.to_datetime(df_game["match_time_utc"], utc=True, errors="coerce")
    df_game["DATETIME_LOCAL"] = df_game.apply(
        lambda r: r["DATETIME_UTC"].astimezone(ZoneInfo(r["timezone"])) if pd.notna(r["DATETIME_UTC"]) else None,
        axis=1
    )
    df_game["DATE_GAME_UTC"] = df_game["DATETIME_UTC"].dt.date.astype(str)
    df_game["TIME_GAME_UTC"] = df_game["DATETIME_UTC"].dt.time.astype(str)
    df_game["DATE_GAME_LOCAL"] = df_game["DATETIME_LOCAL"].dt.date.astype(str)
    df_game["TIME_GAME_LOCAL"] = df_game["DATETIME_LOCAL"].dt.time.astype(str)

    #we extract values from team
    def extract_team(teams, index, field):
        if not isinstance(teams, list) or len(teams) != 2:
            raise ValueError("There is no two teams for each games ")
        return teams[index].get(field)

    df_game["TEAM_HOME"] = df_game["teams"].apply(lambda t: extract_team(t, 0, "team_name"))
    df_game["TEAM_AWAY"] = df_game["teams"].apply(lambda t: extract_team(t, 1, "team_name"))
    df_game["SCORE_HOME"] = df_game["teams"].apply(lambda t: extract_team(t, 0, "score_string"))
    df_game["SCORE_AWAY"] = df_game["teams"].apply(lambda t: extract_team(t, 1, "score_string"))

    df_game["GAME_SOURCE_ID"] = df_game["match_id"]

    # Final selection
    columns = ['COMPETITION_SOURCE', 'COMPETITION_ID', 'SEASON_ID', 'GAMEDAY',
            'DATE_GAME_UTC', 'TIME_GAME_UTC', 'DATE_GAME_LOCAL', 'TIME_GAME_LOCAL',
            'TEAM_HOME', 'SCORE_HOME', 'TEAM_AWAY', 'SCORE_AWAY', 'GAME_SOURCE_ID']
    return df_game[columns].reset_index(drop=True)
    
