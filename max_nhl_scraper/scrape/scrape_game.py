from constants import *
from helpers import *
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime 
import warnings
from typing import Dict, Union
import re

warnings.filterwarnings('ignore')

def scrape_game(game_id: int, pbp_json: Union[Dict, None] = None, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None,
                full_pbp: bool = True) -> Dict:
    
    '''
    Scrape game from NHL API and return a dictionary of dataframes for each table.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    pbp_json : Union[Dict, None], optional
        Play-by-play JSON for game. The default is None.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional
        Shifts dataframe. The default is None.
    full_pbp : bool, optional
        Whether to return full play-by-play dataframe. The default is True.
    '''
    
    pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters
    # html_shifts = fetch_html_shifts(game_id) if html_shifts is None else html_shifts

    html_shifts = fetch_html_shifts(game_id) if html_shifts is None else html_shifts

    gameType = "preseason" if pbp_json.get("gameType", []) == 1 else ("regular-season" if pbp_json.get("gameType", []) == 2 else "playoffs")

    df = (pd.json_normalize(pbp_json.get("plays", []))
           .assign(game_id = game_id,
                      gameType = gameType,
                      season = pbp_json.get("season", []),
                      venue = pbp_json.get("venue", []).get("default", None),
                      startTimeUTC = pbp_json.get("startTimeUTC", []),
                      home_abbr = pbp_json.get("homeTeam", {}).get("abbrev", None),
                      home_name = pbp_json.get("homeTeam", []).get("name", {}).get("default", None),
                      home_logo = pbp_json.get("homeTeam", {}).get("logo", None),
                      away_abbr = pbp_json.get("awayTeam", {}).get("abbrev", None),
                      away_name = pbp_json.get("awayTeam", []).get("name", {}).get("default", None),
                      away_logo = pbp_json.get("awayTeam", {}).get("logo", None),
                      ))
    
    

    df = format_columns(df)
    df = elapsed_time(df)

    df = add_missing_columns(df)
    df = add_event_players_info(df, game_rosters)

    #Column names
    df.columns = [col.split('.')[-1] for col in df.columns]

    if full_pbp :
        df = process_pbp(df, html_shifts, game_rosters,True)
        df = process_pbp(df, html_shifts, game_rosters, False)
        df = strength(df)

        df = df.drop(columns=[ 'winningPlayerId', 'losingPlayerId',
       'hittingPlayerId', 'hitteePlayerId', 'shootingPlayerId',
       'goalieInNetId', 'playerId', 'blockingPlayerId', 'scoringPlayerId',
       'assist1PlayerId', 'assist2PlayerId', 'committedByPlayerId',
       'drawnByPlayerId', 'servedByPlayerId', 'situationCode', 'sortOrder','eventId', 'number',])

    else:
        df = df
        df = df.drop(columns=[ 'winningPlayerId', 'losingPlayerId',
       'hittingPlayerId', 'hitteePlayerId', 'shootingPlayerId',
       'goalieInNetId', 'playerId', 'blockingPlayerId', 'scoringPlayerId',
       'assist1PlayerId', 'assist2PlayerId', 'committedByPlayerId',
       'drawnByPlayerId', 'servedByPlayerId', 'situationCode', 'sortOrder','eventId', 'number',])
    
    return df


if __name__ == '__main__':
    game_id = 2023020005
    df = scrape_game(game_id)
