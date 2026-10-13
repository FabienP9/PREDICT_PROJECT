'''
    The purpose of this module is to extract game details coming from the LNB website 
''' 

import os
import requests
import pandas as pd
from zoneinfo import ZoneInfo
from ...config import config_decorators

@config_decorators.exit_program(log_filter=lambda args: dict(args))
@config_decorators.retry_function(log_filter=lambda args: dict(args))
def get_token(subset_number:int, session: requests.Session) -> str:

    """
        Gets a token from the LNB website to perform games extraction
        Args:
            subset_number (int) : to display it in log
            session (requests.Session) : the session to ask for a token to the LNB website
        Returns:
            the token returned by the LNB website for extracting gammes
        Raises:
            Retry 3 times and exits the program if error with response (using retry decorator)
    """

    url = "https://lnb.fr/api/token"
    response = session.get(url)
    if response.status_code != 200:
        raise ValueError(f"subset_number: {subset_number} | status: {response.status_code} | response: {response.text[:500]}")

    data = response.json()
    token = data.get("token")

    if not token:
        raise ValueError(f"Token missing from response: {data}")

    return token

@config_decorators.exit_program(log_filter=lambda args: dict(args))
@config_decorators.retry_function(log_filter=lambda args: dict(args))
def get_games_subset(subset_number:int, session, payload: dict, anchor_game_id: int | None= None) -> pd.DataFrame:
    
    """
        Gets a subset of games from the LNB website we need to extract
        Args:
            subset_number (int) : to display it in log
            session (requests.Session) : the session to ask for a token to the LNB website
            payload (dict): the dictionary payload to send to LNB website
            anchor_game_id (int): if given, it will get the previous N games on the LNB database. If not, the last N games (N defined by the LNB website)
        Returns:
            the dataframe corresponding to the games extracted
        Raises:
            Retry 3 times and exits the program if error with response (using retry decorator)
    """

    # we first get the token
    token = get_token(subset_number, session)
    if anchor_game_id is not None:
        payload['anchor_match_external_id'] = anchor_game_id
    
    # we then ask for a game subset
    url = "https://api-prod.lnb.fr/match/v3/getCalendar"
    response = session.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {token}"}
    )
    if response.status_code != 200:
        raise ValueError(f"subset_number: {subset_number} | status: {response.status_code} | response: {response.text[:500]}")

    data = response.json().get("data", [])
    df_game_subset = pd.json_normalize(data, record_path="data", errors="ignore")

    return df_game_subset

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def extract_nested_teams_scores(teams_scores: list, index: int, field) -> str | int:

    """
        Exract both teams and both scores from the dataframe returned by LNB
        Args:
            teams_scores (list) : the list of teams or scores
            index (int): the index in the array
            field (str): the name of the field to get the value from
        Returns:
            the dataframe corresponding to the games extracted
        Raises:
            If there is no two teams or two scores, exit the program (using decorators)
    """
    if not isinstance(teams_scores, list) or len(teams_scores) != 2:
        raise ValueError("There is no two teams for each games ")
    return teams_scores[index].get(field)

@config_decorators.exit_program(log_filter=lambda args: dict(args))
def get_game_details_lnb(competition_source_id: int, season_division_number: int, season_name: str, gameday: str | None= None, sr_games_to_extract: pd.Series | None = None) -> pd.DataFrame:

    """
        Gets all games details from a competition coming from LNB website, managed in JSON, possibly filtered by gameday
        Args:
            competition_source_id (int) : get the id of the competition in source
            season_division_number (int): used to filter the request sent to LNB website
            season_name (str): used to filter the request sent to LNB website
            gameday (str): if given, filter the result on this gameday
            sr_games_to_extract (pandas dataframe): if given, filter the result on these games id
        Returns:
            the dataframe corresponding to all games details extracted from this competition and possibly gamedays
        Raises:
            Retry 3 times and exits the program if error with extraction or parsing (using retry decorator)
    """

    #the structure of the final dataframe
    list_columns = ['COMPETITION_SOURCE_ID', 'GAMEDAY',
                    'DATE_GAME_UTC', 'TIME_GAME_UTC', 'DATE_GAME_LOCAL', 'TIME_GAME_LOCAL',
                    'TEAM_HOME', 'SCORE_HOME', 'TEAM_AWAY', 'SCORE_AWAY', 'GAME_SOURCE_ID']
    list_columns_status = list_columns + ['STATUS']
    df_game = pd.DataFrame(columns= list_columns)

    # Parameters to send to LNB website
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "device_type": "web",
    })
    payload = {
        "direction": "previous",
        "division_external_id": str(season_division_number),
        "year": season_name[:4]
    }

    # LNB retrurns data by subset of N games, so we need several iteration, until there is no data remaining to receive
    iteration = 1
    df_game_subset = get_games_subset(iteration,session, payload)

    while (len(df_game_subset) > 0):
        
        df_game_subset["DATETIME_UTC"] = pd.to_datetime(df_game_subset["match_time_utc"], utc=True, errors="coerce")
        df_game_subset["timezone"] = df_game_subset["broadcast"].apply(
            lambda b: b[0]["broadcast_time_zone"] if isinstance(b, list) and b else None
        )
        df_game_subset["DATETIME_LOCAL"] = df_game_subset.apply(
            lambda r: r["DATETIME_UTC"].astimezone(ZoneInfo(r["timezone"]))
            if pd.notna(r["DATETIME_UTC"]) and pd.notna(r.get("timezone"))
            else None,
            axis=1,
        )

        df_game_subset_transformed = pd.DataFrame()
        df_game_subset_transformed['COMPETITION_SOURCE_ID'] = df_game_subset['competition_external_id']
        df_game_subset_transformed["GAMEDAY"] = df_game_subset["round_description"]
        df_game_subset_transformed["DATE_GAME_UTC"] = df_game_subset["DATETIME_UTC"].dt.date.astype(str)
        df_game_subset_transformed["TIME_GAME_UTC"] = df_game_subset["DATETIME_UTC"].dt.time.astype(str) 
        df_game_subset_transformed["DATE_GAME_LOCAL"] = df_game_subset["DATETIME_LOCAL"].dt.date.astype(str)
        df_game_subset_transformed["TIME_GAME_LOCAL"] = df_game_subset["DATETIME_LOCAL"].dt.time.astype(str)
        df_game_subset_transformed["TEAM_HOME"] = df_game_subset["teams"].apply(lambda t: extract_nested_teams_scores(t, 0, "team_name"))
        df_game_subset_transformed["TEAM_AWAY"] = df_game_subset["teams"].apply(lambda t: extract_nested_teams_scores(t, 1, "team_name"))
        df_game_subset_transformed["SCORE_HOME"] = df_game_subset["teams"].apply(lambda t: extract_nested_teams_scores(t, 0, "score_string"))
        df_game_subset_transformed["SCORE_AWAY"] = df_game_subset["teams"].apply(lambda t: extract_nested_teams_scores(t, 1, "score_string"))
        df_game_subset_transformed["GAME_SOURCE_ID"] = df_game_subset["match_id"]
        df_game_subset_transformed['STATUS'] = df_game_subset["match_status"]

        df_game_subset_transformed = df_game_subset_transformed[list_columns_status].reset_index(drop=True)
        df_game = pd.concat([df_game, df_game_subset_transformed], ignore_index=True)
        
        anchor_game_id = df_game_subset.loc[0, 'external_id'].item()
        iteration = iteration +1
        df_game_subset = get_games_subset(iteration,session,payload, anchor_game_id)
    
    df_game = df_game[df_game["COMPETITION_SOURCE_ID"] == competition_source_id].reset_index(drop=True)
    if gameday is not None:
        df_game = df_game[df_game["GAMEDAY"] == gameday].reset_index(drop=True)
    if sr_games_to_extract is not None:
        df_game = df_game[df_game["GAME_SOURCE_ID"].astype(str).isin(sr_games_to_extract.values.astype(str))]
    if not df_game['STATUS'].isin(['SCHEDULED','COMPLETE']).all() and os.getenv('OVERWRITE_GAMES_STATUS') == 0:
        raise ValueError("At least one game is in progress or unknow status- retry extraction later")
    df_game = df_game[(df_game['TEAM_HOME'].notna()) & (df_game['TEAM_AWAY'].notna())]  
    df_game = df_game[list_columns].reset_index(drop=True)
    return df_game