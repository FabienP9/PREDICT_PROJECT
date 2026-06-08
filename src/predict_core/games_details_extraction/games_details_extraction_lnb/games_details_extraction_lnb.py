'''
    The purpose of this module is to extract game details coming from the LNB website 
''' 

import os
from zoneinfo import ZoneInfo
import pandas as pd
import requests

from ...config import config_decorators

@config_decorators.exit_program(log_filter=lambda args: dict(args))
@config_decorators.retry_function(log_filter=lambda args: dict(args))
def get_game_details_lnb(competition_source_id: int, gameday: str | None= None, sr_games_to_extract: pd.Series | None = None) -> pd.DataFrame:

    """
        Gets all games details from a competition coming from LNB website, managed in JSON, possibly filtered by gameday
        Args:
            competition_source_id (int) : get the id of the competition in source
            gameday (str): if given, filter on this gameday
            sr_games_to_extract (pandas dataframe): if given, filter on these games id
        Returns:
            the dataframe corresponding to all games details extracted from this competition and possibly gamedays
        Raises:
            Retry 3 times and exits the program if error with extraction or parsing (using retry decorator)
    """

    url = "https://api-prod.lnb.fr/match/getCalendar"
    payload = {
        "competition_external_id": int(competition_source_id),
        "start_date": "2000-01-01",
        "end_date": "2999-12-31"
    }
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Ch-Ua": '"Not(A:Brand";v="99", "Google Chrome";v="123", "Chromium";v="123"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site"
    }
    response = requests.post(url, json=payload, headers=headers)
    data = response.json().get("data", [])
    df_game = pd.json_normalize(data, record_path="data", errors="ignore")

    if gameday is not None:
          df_game = df_game[df_game["round_description"].astype(str) == gameday]
    elif sr_games_to_extract is not None:
          df_game = df_game[df_game["match_id"].astype(str).isin(sr_games_to_extract.values.astype(str))]

    game_status = df_game["match_status"]
    if not game_status.isin(['SCHEDULED','COMPLETE']).all() and os.getenv('OVERWRITE_GAMES_STATUS') == 0:
        raise ValueError("At least one game is in progress or unknow status- retry extraction later")
    
    df_game['COMPETITION_SOURCE_ID'] = competition_source_id
    df_game["GAMEDAY"] = df_game["round_description"]

    # we get datetime of game
    df_game["DATETIME_UTC"] = pd.to_datetime(df_game["match_time_utc"], utc=True, errors="coerce")
    df_game["DATETIME_LOCAL"] = df_game.apply(
        lambda r: r["DATETIME_UTC"].astimezone(ZoneInfo(r["timezone"])) if pd.notna(r["DATETIME_UTC"]) else None,
        axis=1
    )
    df_game["DATETIME_LOCAL"] = pd.to_datetime(df_game["DATETIME_LOCAL"], errors="coerce")
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
    columns = ['COMPETITION_SOURCE_ID', 'GAMEDAY',
            'DATE_GAME_UTC', 'TIME_GAME_UTC', 'DATE_GAME_LOCAL', 'TIME_GAME_LOCAL',
            'TEAM_HOME', 'SCORE_HOME', 'TEAM_AWAY', 'SCORE_AWAY', 'GAME_SOURCE_ID']
    
    return df_game[columns].reset_index(drop=True)
    
