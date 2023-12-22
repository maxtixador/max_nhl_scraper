#Imports

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime 
import warnings
from typing import Dict, Union

warnings.filterwarnings('ignore')

#Constants
NHL_API_BASE_URL_1 = 'https://api-web.nhle.com/v1'

PLAY_BY_PLAY_ENDPOINT = f'{NHL_API_BASE_URL_1}/gamecenter/{{game_id}}/play-by-play'

SCHEDULE_ENDPOINT = f'{NHL_API_BASE_URL_1}/club-schedule-season/{{team_abbr}}/{{season}}'

SHIFT_REPORT_HOME_ENDPOINT = 'http://www.nhl.com/scores/htmlreports/{season}/TH{game_id}.HTM'
SHIFT_REPORT_AWAY_ENDPOINT = 'http://www.nhl.com/scores/htmlreports/{season}/TV{game_id}.HTM'

SHIFT_API_ENDPOINT = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={{game_id}}"


DEFAULT_SEASON = 20232024
DEFAULT_TEAM = "MTL"

NUMERICAL_COLUMNS = ['period', 'xCoord', 'yCoord', 'awayScore', 'homeScore', 'awaySOG','homeSOG', 'duration', 'event_player1_id', 'event_player2_id', 'event_player3_id', 'opposing_goalie_id', "game_id"]

CATEGORICAL_COLUMNS = ['homeTeamDefendingSide', 'typeDescKey', 'periodType',  'zoneCode', 'reason', 'shotType',  'typeCode', 'descKey', 'secondaryReason', "gameType", "venue", "season"]

def filter_players(players, side):
    if side is not None:
        side = side.lower()
        filter_condition = "is_home == 1" if side == "home" else "is_home == 0"
        players = players.query(filter_condition)
    return players

def str_to_sec(value):
    # Split the time value into minutes and seconds
    minutes, seconds = value.split(':')

    # Convert minutes and seconds to integers
    minutes = int(minutes)
    seconds = int(seconds)

    # Calculate the total seconds
    return minutes * 60 + seconds

def format_df(df):

    #Column names
    df.columns = [col.split('.')[-1] for col in df.columns]

    #Numerical columns
    df[NUMERICAL_COLUMNS] = df[NUMERICAL_COLUMNS].apply(pd.to_numeric, errors='coerce')

    #Category columns
    df[CATEGORICAL_COLUMNS] = df[CATEGORICAL_COLUMNS].astype("category")

    #Date and time cols
    df['startTimeUTC'] = pd.to_datetime(df['startTimeUTC'])#.dt.date to get only date


    # Apply the str_to_sec function to create the "timeInPeriod_s" column
    df[["timeInPeriod_s", 'timeRemaining_s']] = df[["timeInPeriod", 'timeRemaining']].map(str_to_sec)


    return df

def elapsed_time(df):

    # Calculate the elapsed time in seconds based on gameType and period
    df['elapsedTime'] = df['timeInPeriod_s'] + 60*(df['period'] - 1) * 20

    df.loc[(df['period'] >= 5) & (df["gameType"] != "playoffs"), 'elapsedTime'] = np.nan 

    return df

def add_missing_columns(df):
    cols_to_add = [
        "details.winningPlayerId", "details.losingPlayerId", "details.hittingPlayerId", "details.hitteePlayerId",
        "details.shootingPlayerId", "details.goalieInNetId", "details.playerId", "details.blockingPlayerId",
        "details.scoringPlayerId", "details.assist1PlayerId", "details.assist2PlayerId",
        "details.committedByPlayerId", "details.drawnByPlayerId", "details.servedByPlayerId",
        "situationCode", "typeCode", "sortOrder", "eventId", 'periodDescriptor.number'
    ] 

    for col in cols_to_add:
        if col not in df.columns:
            df[col] = np.nan
    return df

def format_columns(df):

    #Adding cols
    cols =  [
    "details.winningPlayerId", "details.losingPlayerId",
    "details.hittingPlayerId", "details.hitteePlayerId",
    "details.shootingPlayerId", "details.goalieInNetId",
    "details.playerId", "details.blockingPlayerId",
    "details.scoringPlayerId", "details.assist1PlayerId",
    "details.assist2PlayerId", "details.committedByPlayerId",
    "details.drawnByPlayerId", "details.servedByPlayerId",
    "situationCode", "typeCode", "sortOrder", "eventId", 'periodDescriptor.number']

    # Calculate the set difference to find missing columns
    columns_missing = set(cols) - set(df.columns)

    # Add missing columns with default values (e.g., None)
    for column in columns_missing:
        df[column] = np.nan

    #Faceoff
    df.loc[df["typeDescKey"] == 'faceoff', "event_player1_id"] = df["details.winningPlayerId"] #Winner
    df.loc[df["typeDescKey"] == 'faceoff', "event_player2_id"] = df["details.losingPlayerId"] #Loser

    #Hit
    df.loc[df["typeDescKey"] == 'hit', "event_player1_id"] = df["details.hittingPlayerId"] #Hitter
    df.loc[df["typeDescKey"] == 'hit', "event_player2_id"] = df["details.hitteePlayerId"] #Hittee

    #Missed shot & shot on goal
    df.loc[df["typeDescKey"].isin(['missed-shot', 'shot-on-goal', 'failed-shot-attempt']), "event_player1_id"] = df["details.shootingPlayerId"] #Shooter
    df.loc[df["typeDescKey"].isin(['missed-shot', 'shot-on-goal', 'failed-shot-attempt']), "event_player2_id"] = df["details.goalieInNetId"] #Goalie

    #Giveaway & Takeaway & Failed shot attempt (SO)
    ### Gotta investigate if failed penalty shot attempt is also a failed shot attempt ###
    df.loc[df["typeDescKey"].isin(['giveaway','takeaway']), "event_player1_id"] = df["details.playerId"] #Player

    #Blocked shot
    df.loc[df["typeDescKey"]== 'blocked-shot', "event_player1_id"] = df["details.shootingPlayerId"] #Shooter
    df.loc[df["typeDescKey"]== 'blocked-shot', "event_player2_id"] = df["details.blockingPlayerId"] #Blocker

    #Goal
    df.loc[df["typeDescKey"] == 'goal', "event_player1_id"] = df["details.scoringPlayerId"] #Goal-scorer
    df.loc[df["typeDescKey"] == 'goal', "event_player2_id"] = df["details.assist1PlayerId"] #1stPasser
    df.loc[df["typeDescKey"] == 'goal', "event_player3_id"] = df["details.assist2PlayerId"] #2ndPasser

    #Penalty
    df.loc[df["typeDescKey"] == 'penalty', "event_player1_id"] = df["details.committedByPlayerId"] #Penalized
    df.loc[df["typeDescKey"] == 'penalty', "event_player2_id"] = df["details.drawnByPlayerId"] #Drawer
    df.loc[df["typeDescKey"] == 'penalty', "event_player3_id"] = df["details.servedByPlayerId"] #Server

    #Opposing goalie
    df["opposing_goalie_id"] = df["details.goalieInNetId"]


    df = df.drop(["details.winningPlayerId", "details.losingPlayerId",
             "details.hittingPlayerId", "details.hitteePlayerId",
             "details.shootingPlayerId", "details.goalieInNetId",
             "details.playerId", "details.blockingPlayerId",
             "details.scoringPlayerId", "details.assist1PlayerId", "details.assist2PlayerId",
             "details.committedByPlayerId", "details.drawnByPlayerId", "details.servedByPlayerId",
             "situationCode", "typeCode", "sortOrder", "eventId", 'periodDescriptor.number', 'details.eventOwnerTeamId'
             ], axis=1)

    # Renaming columns
    df.columns = [col.split('.')[-1] for col in df.columns]

    # Converting columns to appropriate data types
    df[NUMERICAL_COLUMNS] = df[NUMERICAL_COLUMNS].apply(pd.to_numeric, errors='coerce')
    df[CATEGORICAL_COLUMNS] = df[CATEGORICAL_COLUMNS].astype("category")
    df['startTimeUTC'] = pd.to_datetime(df['startTimeUTC'])#.dt.date to get only date
    df[["timeInPeriod_s", 'timeRemaining_s']] = df[["timeInPeriod", 'timeRemaining']].map(str_to_sec)
    df = elapsed_time(df)
    return df

def add_event_players_info(df, rosters_df):
    p_df = rosters_df.copy()
    df = (df.merge(
        (p_df[['playerId', 'fullName','abbrev', 'positionCode']].rename(columns={'playerId':'event_player1_id',
                                                                      'fullName':'event_player1_fullName',
                                                                      'abbrev' : 'event_player1_team',
                                                                      'positionCode' : 'event_player1_position'})),
        on="event_player1_id",how="left"
    )
    .merge(
        (p_df[['playerId', 'fullName','abbrev', 'positionCode']].rename(columns={'playerId':'event_player2_id',
                                                                      'fullName':'event_player2_fullName',
                                                                      'abbrev' : 'event_player2_team',
                                                                      'positionCode' : 'event_player2_position'})),
        on="event_player2_id",how="left"
    )
    .merge(
        (p_df[['playerId', 'fullName','abbrev', 'positionCode']].rename(columns={'playerId':'event_player3_id',
                                                                      'fullName':'event_player3_fullName',
                                                                      'abbrev' : 'event_player3_team',
                                                                      'positionCode' : 'event_player3_position'})),
        on="event_player3_id",how="left"
    )
    .merge(
        (p_df[['playerId', 'fullName','abbrev', 'positionCode']].rename(columns={'playerId':'opposing_goalie_id',
                                                                      'fullName':'opposing_goalie_fullName',
                                                                      'abbrev' : 'opposing_goalie_team',
                                                                      'positionCode' : 'opposing_goalie_position'})),
        on="opposing_goalie_id",how="left"
    )
    )
    df["event_team"] = df["event_player1_team"]
    df = df.rename(columns={"typeDescKey" : "event"})
    df["is_home"] = np.nan
    df.loc[df["event_team"] == df["home_abbr"],"is_home"] = 1
    df.loc[df["event_team"] == df["away_abbr"],"is_home"] = 0


    return df

def strength(df):

    ### FIX GAME STRENGTH ###

    ### THIS EXEMPLE scrape_game(2023020069) HAS WRONG GAME STRENGTH FOR GAME VS CAPS (5V5 IN OT) ###


    df['home_skaters'] = (~df[['home_on_position_1', 'home_on_position_2', 'home_on_position_3', 'home_on_position_4', 'home_on_position_5', 'home_on_position_6', 'home_on_position_7']].isin(['G', np.nan])).sum(axis=1)
    df['away_skaters'] = (~df[['away_on_position_1', 'away_on_position_2', 'away_on_position_3', 'away_on_position_4', 'away_on_position_5', 'away_on_position_6', 'away_on_position_7']].isin(['G', np.nan])).sum(axis=1)

    df["strength"] = np.where(df["event_team"] == df['home_abbr'], df['home_skaters'].astype(str) + 'v' + df['away_skaters'].astype(str), df['away_skaters'].astype(str) + 'v' + df['home_skaters'].astype(str))

    df = df.strength.replace({'0v0': None})


    return df

def process_pbp(pbp, shifts_df, rosters_df, is_home=True):
    is_home = int(is_home)
    # print(is_home)
    place = 'home' if is_home else 'away'

    # players = rosters_df.query("is_home==@is_home").set_index('sweaterNumber')['playerId'].to_dict()
    # print(players)

    shifts_df = shifts_df.query("is_home==@is_home").query('duration_s > 0').copy()
    players_on = []

    # print(shifts_df)
    for _, row in pbp.iterrows():
        current_time = row['elapsedTime']
        if pd.isna(row['event_team']):
            players_on.append(np.nan)
        # elif row['event'] == 'faceoff':
        #### You should get rid of the elif row['event'] == 'faceoff': branch in process_pbp. Faceoffs don't have to come at the start of a shift. Seems like that cleans up a lot of it.   

        #     # current_time = row['elapsedTime']
        #     # print(current_time)
        #     players_on_ice = shifts_df.query('startTime_s == @current_time')['playerId'].unique().tolist()
            
        #     # players_on_ice_2 = [players.get(int(item), int(item)) for item in players_on_ice]
        #     # print(players_on_ice)
        #     players_on.append(players_on_ice)
            
        # elif row['event'] == 'goal':
        #     players_on_ice = shifts_df.query('startTime_s < @current_time and endTime_s >= @current_time')['playerId'].unique().tolist()
            
        #     # players_on_ice_2 = [players.get(int(item), int(item)) for item in players_on_ice]
        #     # print(players_on_ice)
        #     players_on.append(players_on_ice)

        else:
            # current_time = row['elapsedTime']
            # print(current_time)
            # players_on_ice = shifts_df.query('startTime_s =< @current_time and endTime_s >= @current_time')['playerId'].unique().tolist()
            players_on_ice = shifts_df.query('startTime_s < @current_time and endTime_s >= @current_time')['playerId'].unique().tolist()
            # players_on_ice_2 = [players.get(int(item), int(item)) for item in players_on_ice]
            # print(players_on_ice)
            players_on.append(players_on_ice)
            if len(players_on_ice) > 7:
                print(row['game_id'],players_on_ice, current_time, row['event'])
    
    pbp[f'{place}_on'] = players_on

    max_list_length = pbp[f'{place}_on'].apply(lambda x: len(x) if isinstance(x, list) else 0).max()

    for i in range(max_list_length):
        pbp[f'{place}_on_id_{i+1}'] = np.nan

    for index, row in pbp.iterrows():
        values = row[f'{place}_on']
        if isinstance(values, list):
            for i, value in enumerate(values):
                pbp.at[index, f'{place}_on_id_{i+1}'] = value
                pbp.at[index, f'{place}_on_name_{i+1}'] = value
                pbp.at[index, f'{place}_on_position_{i+1}'] = value



    pbp[f"{place}_on_id_7"] = np.nan if f"{place}_on_id_7" not in pbp.columns else pbp[f"{place}_on_id_7"]

    pbp[f"{place}_on_name_1"], pbp[f"{place}_on_name_2"], pbp[f"{place}_on_name_3"], pbp[f"{place}_on_name_4"], pbp[f"{place}_on_name_5"], pbp[f"{place}_on_name_6"], pbp[f"{place}_on_name_7"] = pbp[f"{place}_on_id_1"], pbp[f"{place}_on_id_2"], pbp[f"{place}_on_id_3"], pbp[f"{place}_on_id_4"], pbp[f"{place}_on_id_5"], pbp[f"{place}_on_id_6"], pbp[f"{place}_on_id_7"]

    players_id = rosters_df.query("is_home==@is_home").set_index('playerId')['fullName'].to_dict()
    # Define the columns to be replaced
    columns_to_replace = [f"{place}_on_name_1", f"{place}_on_name_2", f"{place}_on_name_3", f"{place}_on_name_4", f"{place}_on_name_5", f"{place}_on_name_6", f"{place}_on_name_7"]

    # Use the replace method to replace player IDs with names
    pbp[columns_to_replace] = pbp[columns_to_replace].replace(players_id) 



    pbp[f"{place}_on_position_1"], pbp[f"{place}_on_position_2"], pbp[f"{place}_on_position_3"], pbp[f"{place}_on_position_4"], pbp[f"{place}_on_position_5"], pbp[f"{place}_on_position_6"], pbp[f"{place}_on_position_7"] = pbp[f"{place}_on_id_1"], pbp[f"{place}_on_id_2"], pbp[f"{place}_on_id_3"], pbp[f"{place}_on_id_4"], pbp[f"{place}_on_id_5"], pbp[f"{place}_on_id_6"], pbp[f"{place}_on_id_7"]

    players_id = rosters_df.query("is_home==@is_home").set_index('playerId')['positionCode'].to_dict()
    # Define the columns to be replaced
    columns_to_replace = [f"{place}_on_position_1", f"{place}_on_position_2", f"{place}_on_position_3", f"{place}_on_position_4", f"{place}_on_position_5", f"{place}_on_position_6", f"{place}_on_position_7"]

    # Use the replace method to replace player IDs with names
    pbp[columns_to_replace] = pbp[columns_to_replace].replace(players_id) 

    pbp = pbp.drop([f"{place}_on"], axis=1)
    pbp=pbp.loc[:, ~pbp.columns[::-1].duplicated()[::-1]]

    return pbp

#Fetch scripts

def fetch_play_by_play_json(game_id: int) -> Dict:
    """
    Connects to the NHL API to get the data for a given game.

    Args:
      game_id: Identifier ID for a given game.

    Returns:
      A JSON file with the information of the game.

    Raises:
      requests.exceptions.RequestException: If there's an issue with the request.
    """
    response = requests.get(PLAY_BY_PLAY_ENDPOINT.format(game_id=game_id))
    response.raise_for_status()  # Raise an error for bad responses.
    return response.json()

def fetch_team_schedule_json(team_abbr: str = DEFAULT_TEAM, season: int = DEFAULT_SEASON) -> Dict:
    """
    Connects to the NHL API to get the data for a given team's schedule.

    Args:
      team_abbr: Team abbreviation.
      season: Desired season in the format of {year_start}{year_end}.

    Returns:
      A JSON file with the schedule of a given team.

    Raises:
      requests.exceptions.RequestException: If there's an issue with the request.
    """
    response = requests.get(SCHEDULE_ENDPOINT.format(team_abbr=team_abbr, season=season))
    response.raise_for_status()
    return response.json()

def fetch_game_rosters(game_id: int, side: Union[str, None] = None, pbp_json: Union[Dict, None] = None) -> pd.DataFrame:
    """
    Fetches and processes rosters of both teams for a given game.

    Args:
      game_id: Identifier ID for a given game.
      side: To filter for the 'home' or away team. Default is None, meaning no filtering.
      pbp_json: JSON file of the Play-by-Play data of the game. Defaulted to None.

    Returns:
      A Pandas DataFrame with the rosters of both teams who played the game and information about the players.
    """
    
    pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json


    players = pd.json_normalize(pbp_json.get("rosterSpots", [])).filter(['teamId', 'playerId', 'sweaterNumber', 'positionCode', 'headshot',
       'firstName.default', 'lastName.default']).rename(columns={'lastName.default':'lastName',
                  'firstName.default':'firstName'}).rename(columns={"id":"teamId","name":"team"})
    home_team, away_team = pd.json_normalize(pbp_json.get("homeTeam", [])), pd.json_normalize(pbp_json.get("awayTeam", []))
    teams = pd.concat([home_team.assign(is_home=1), away_team.assign(is_home=0)]).rename(columns={"id":"teamId", "name":"team"})
    players = players.merge(teams[["teamId", "abbrev", "is_home"]], on="teamId", how="left")
    players["fullName"] = players['firstName'] + " " + players['lastName']
    players["playerId"] = pd.to_numeric(players["playerId"])
    players["game_id"] = game_id

    return filter_players(players, side)

def fetch_api_shifts(game_id, pbp_json=None):
    '''
    Fetches shifts data from the NHL API and returns a DataFrame with the data.
    ----
    :param game_id: The game ID of the game to fetch shifts for.
    :param pbp_json: The play-by-play JSON for the game. If not provided, it will be fetched from the API.
    :return: A DataFrame containing the shifts data for the game.
    '''


    # Fetch play-by-play data
    pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json

    home_team_abbrev = pbp_json["homeTeam"]["abbrev"]
    # away_team_abbrev = pbp_json["awayTeam"]["abbrev"]

    # Fetch shifts data from the API
    shifts_data = requests.get(SHIFT_API_ENDPOINT.format(game_id=game_id)).json()['data']

    # Create a DataFrame and perform data transformations
    shift_df = pd.json_normalize(shifts_data)
    shift_df = shift_df.drop(columns=['id', 'detailCode', 'eventDescription', 'eventDetails', 'eventNumber', 'typeCode'])
    shift_df['fullName'] = shift_df['firstName'] + " " + shift_df['lastName']
    shift_df['duration_s'] = shift_df['duration'].fillna('00:00').apply(str_to_sec)
    shift_df['startTime_s'] = shift_df['startTime'].apply(str_to_sec) + 60 * (shift_df['period'] - 1) * 20
    shift_df['endTime_s'] = shift_df['endTime'].apply(str_to_sec) + 60 * (shift_df['period'] - 1) * 20
    shift_df['teamAbbrev'] = shift_df['teamAbbrev'].str.strip()
    shift_df['is_home'] = np.where(shift_df['teamAbbrev'] == home_team_abbrev, 1, 0)

    # Filter and select relevant columns
    columns_to_select = [
        'playerId', 'fullName', 'teamAbbrev', 'startTime_s', 'endTime_s', 'duration_s',
        'period', 'startTime', 'endTime', 'duration', 'firstName', 'lastName',
        'teamName', 'teamId', 'shiftNumber', 'gameId', 'hexValue', 'is_home'
    ]
    shift_df = shift_df[columns_to_select]

    shift_df["type"] = "OTF"

    faceoffs = (pd.json_normalize(pbp_json["plays"])
                .query('typeDescKey=="faceoff"')
                .filter(['timeInPeriod','homeTeamDefendingSide', 'details.xCoord','details.zoneCode', 'period'])
                .assign(current_time = lambda x: x['timeInPeriod'].apply(str_to_sec) +20*60* (x['period']-1))
                .drop(columns=['timeInPeriod', 'period']))

    

    for _, shift in shift_df.iterrows():

        time = shift["startTime_s"]
        if time in faceoffs["current_time"].values:
            matching_faceoffs = faceoffs.query("current_time == @time")
            zoneCode = matching_faceoffs["details.zoneCode"].values[0]
            homeTeamZone = matching_faceoffs["homeTeamDefendingSide"].values[0]
            xCoord = matching_faceoffs["details.xCoord"].values[0]



            if zoneCode == "N":
                shift_df.at[_, "type"] = "NZF"
            elif (
                homeTeamZone == "left" and shift["is_home"] == 1 and xCoord < 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 1 and xCoord > 0
            ) or (
                homeTeamZone == "left" and shift["is_home"] == 0 and xCoord > 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 0 and xCoord < 0
            ):
                shift_df.at[_, "type"] = "DZF"
            elif (
                homeTeamZone == "left" and shift["is_home"] == 1 and xCoord > 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 1 and xCoord < 0
            ) or (
                homeTeamZone == "left" and shift["is_home"] == 0 and xCoord < 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 0 and xCoord > 0
            ):
                shift_df.at[_, "type"] = "OZF"
        else:
            shift_df.at[_, "type"] = "OTF"

    shift_df['date'] = pbp_json['gameDate']
    shift_df['season'] = pbp_json['season']
    shift_df['gameType'] = game_id
    

    return shift_df

def fetch_html_shifts(game_id=2023020069, season=None, pbp_json=None):
    ''' 
    Fetches shifts data from the NHL API and returns a DataFrame with the data.
    ----
    :param game_id: The game ID of the game to fetch shifts for.
    :param season: The season of the game. If not provided, it will be fetched from the API.
    :param pbp_json: The play-by-play JSON for the game. If not provided, it will be fetched from the API.
    :return: A DataFrame containing the shifts data for the game.
    '''

    pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json
    rosters = fetch_game_rosters(game_id)

    season = f"{str(game_id)[:4]}{int(str(game_id)[:4]) + 1}" if season is None else season


    ### HOME SHIFTS ###
    url = SHIFT_REPORT_HOME_ENDPOINT.format(season=season, game_id=str(game_id)[4:])
    page = (requests.get(url))
    soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None, from_encoding='utf-8')
    found = soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})
    if len(found)==0:
        raise IndexError('This game has no shift data.')
    thisteam = soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()
    

    players = dict()
    for i in range(len(found)):
        line = found[i].get_text()
        if ', ' in line:
            name = line.split(',')
            number = name[0].split(' ')[0].strip()
            last_name =  name[0].split(' ')[1].strip()
            first_name = name[1].strip()
            full_name = first_name + " " + last_name
            players[full_name] = dict()
            players[full_name]['number'] = number
            players[full_name]['name'] = full_name
            players[full_name]['shifts'] = []
        else:
            players[full_name]['shifts'].extend([line])

    alldf = pd.DataFrame()

    for key in players.keys(): 
        length = int(len(np.array((players[key]['shifts'])))/5)
        df = pd.DataFrame(np.array((players[key]['shifts'])).reshape(length, 5)).rename(
        columns = {0:'shift_number', 1:'period', 2:'shift_start', 3:'shift_end', 4:'duration'})
        df = df.assign(name = players[key]['name'],
                      sweaterNumber = int(players[key]['number']),
                      team = thisteam,
                      is_home = 1)
        alldf = pd.concat([alldf, df], ignore_index=True)
        
    home_shifts = alldf

    ### AWAY SHIFTS ###
    url = SHIFT_REPORT_AWAY_ENDPOINT.format(season=season, game_id=str(game_id)[4:])
    page = (requests.get(url))
    soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None, from_encoding='utf-8')
    found = soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})
    if len(found)==0:
        raise IndexError('This game has no shift data.')
    thisteam = soup.find('td', {'align':'center', 'class':'teamHeading + border'}).get_text()
    

    players = dict()
    for i in range(len(found)):
        line = found[i].get_text()
        if ', ' in line:
            name = line.split(',')
            number = name[0].split(' ')[0].strip()
            last_name =  name[0].split(' ')[1].strip()
            first_name = name[1].strip()
            full_name = first_name + " " + last_name
            players[full_name] = dict()
            players[full_name]['number'] = number
            players[full_name]['name'] = full_name
            players[full_name]['shifts'] = []
        else:
            players[full_name]['shifts'].extend([line])

    alldf = pd.DataFrame()

    for key in players.keys(): 
        length = int(len(np.array((players[key]['shifts'])))/5)
        df = pd.DataFrame(np.array((players[key]['shifts'])).reshape(length, 5)).rename(
        columns = {0:'shift_number', 1:'period', 2:'shift_start', 3:'shift_end', 4:'duration'})
        df = df.assign(name = players[key]['name'],
                      sweaterNumber = int(players[key]['number']),
                      team = thisteam,
                      is_home = 0)
        alldf = pd.concat([alldf, df], ignore_index=True)
        
    away_shifts = alldf

    ### MERGE SHIFTS ###
    all_shifts = (pd.concat([home_shifts, away_shifts], ignore_index=True)
                  .drop(columns=['name', 'team'])
                  .merge(rosters, how='left', on=['sweaterNumber', 'is_home']))


    all_shifts[['startTime', 'startTime_remaning']] = all_shifts['shift_start'].str.split(' / ', expand=True)

    # Split 'shift_end' column into two columns
    all_shifts[['endTime', 'endTime_remaning']] = all_shifts['shift_end'].str.split(' / ', expand=True)    

    all_shifts = all_shifts.drop(columns=[ 'startTime_remaning',  'endTime_remaning', 'shift_start', 'shift_end']).replace({'OT':4})

    all_shifts['period'] = all_shifts['period'].astype(int)
    all_shifts['duration_s'] = all_shifts['duration'].fillna('00:00').apply(str_to_sec)
    all_shifts['startTime_s'] = all_shifts['startTime'].apply(str_to_sec) + 60 * (all_shifts['period'] - 1) * 20
    all_shifts['endTime_s'] = all_shifts['endTime'].apply(str_to_sec) + 60 * (all_shifts['period'] - 1) * 20
    
    all_shifts["type"] = "OTF"

    faceoffs = (pd.json_normalize(pbp_json["plays"])
                .query('typeDescKey=="faceoff"')
                .filter(['timeInPeriod','homeTeamDefendingSide', 'details.xCoord','details.zoneCode', 'period'])
                .assign(current_time = lambda x: x['timeInPeriod'].apply(str_to_sec) +20*60* (x['period']-1))
                .drop(columns=['timeInPeriod', 'period']))

    
    #
    for _, shift in all_shifts.iterrows():

        time = shift["startTime_s"]
        if time in faceoffs["current_time"].values:
            matching_faceoffs = faceoffs.query("current_time == @time")
            zoneCode = matching_faceoffs["details.zoneCode"].values[0]
            homeTeamZone = matching_faceoffs["homeTeamDefendingSide"].values[0]
            xCoord = matching_faceoffs["details.xCoord"].values[0]



            if zoneCode == "N":
                all_shifts.at[_, "type"] = "NZF"
            elif (
                homeTeamZone == "left" and shift["is_home"] == 1 and xCoord < 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 1 and xCoord > 0
            ) or (
                homeTeamZone == "left" and shift["is_home"] == 0 and xCoord > 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 0 and xCoord < 0
            ):
                all_shifts.at[_, "type"] = "DZF"
            elif (
                homeTeamZone == "left" and shift["is_home"] == 1 and xCoord > 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 1 and xCoord < 0
            ) or (
                homeTeamZone == "left" and shift["is_home"] == 0 and xCoord < 0
            ) or (
                homeTeamZone == "right" and shift["is_home"] == 0 and xCoord > 0
            ):
                all_shifts.at[_, "type"] = "OZF"
        else:
            all_shifts.at[_, "type"] = "OTF"

    all_shifts['date'] = pbp_json['gameDate']
    all_shifts['season'] = pbp_json['season']
    all_shifts['gameType'] = game_id


    return all_shifts


#Scrape game

### STILL HAVE TO CLEAN UP THE COLUMNS OF THE DATAFRAME ###
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


#Get the TOI per player per strength for a given game.
def get_strength_toi_per_team(game_id=2023020005, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None):

    ''' 
    Get the TOI per strength for a given game.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional
        Shifts dataframe. The default is None.
    is_home : bool, optional
        Whether to get the home or away players. The default is True.
    '''

    html_shifts = fetch_html_shifts(game_id) if html_shifts is None else html_shifts
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters

    is_home = 1
    place = 'home' if is_home else 'away'
    not_place = 'away' if is_home else 'home'

    home_player_counts = get_player_count_per_second(game_id, html_shifts=html_shifts, is_home=1) #Gotta work on a fix with game_rosters
    away_player_counts = get_player_count_per_second(game_id, html_shifts=html_shifts, is_home=(0)) #Gotta work on a fix with game_rosters

    df = home_player_counts.merge(away_player_counts, on=["Second", "game_id"], how="left")

    df['is_home2'] = 1 if is_home else 0
    df['home_strength'] = df.apply(lambda row: f'{row[f"{place}Count"]}v{row[f"{not_place}Count"]}', axis=1)
    df['away_strength'] = df.apply(lambda row: f'{row[f"{not_place}Count"]}v{row[f"{place}Count"]}', axis=1)

    df = pd.concat([(df.home_strength
            .value_counts()
            .reset_index()
            .assign(is_home=1)
            .rename(columns={"home_strength" : "strength", "count" : "TOI"})),
            (df.away_strength
            .value_counts()
            .reset_index()
            .assign(is_home=0)
            .rename(columns={"away_strength" : "strength", "count" : "TOI"}))])
    
    pbp_json = fetch_play_by_play_json(game_id)

    df["abbrev"] = pbp_json['homeTeam']["abbrev"]
    df["name"] = pbp_json['homeTeam']["name"]

    df.loc[df['is_home']==0, 'abbrev'] = pbp_json['awayTeam']['abbrev']
    df.loc[df['is_home']==0, 'name'] = pbp_json['awayTeam']['name']

    df["game_id"] = game_id

    df = df.query("strength not in ['0v0']").reset_index(drop=True)

    
    return df

#TOI Manips
def get_player_count_per_second(game_id=2023020005, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None, is_home=True):
    '''
    Get the number of players on the ice per second for a given game.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional
        Shifts dataframe. The default is None.
    is_home : bool, optional
        Whether to get the home or away players. The default is True.
    '''

    place = 'home' if is_home else 'away'

    html_shifts = fetch_html_shifts(game_id) if html_shifts is None else html_shifts
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters

    

    df = html_shifts.copy().query("is_home==@is_home")

    # Create a time-based range
    game_duration = df['endTime_s'].max()  # Assumes the endTime_s column represents the game duration
    time_range = range(game_duration)  # +1 to include the last second

    # Create a DataFrame with all seconds in the game
    time_df = pd.DataFrame({'Second': time_range})

    # Calculate player counts for each second
    def count_players_on_ice(second):
        on_ice = df[(second >= df['startTime_s']) & (second < df['endTime_s'])
                    & (df['positionCode'].isin(['C', 'D', 'L', 'R']))]['sweaterNumber'].nunique()  # Adjust position codes as needed
        return on_ice

    time_df[f'{place}Count'] = time_df['Second'].apply(count_players_on_ice)

    time_df["game_id"] = game_id

    # Print the resulting DataFrame
    return time_df

def get_player_ids_per_second(game_id=2023020005, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None, is_home=True):
    
    '''
    Get the player IDs on the ice per second for a given game.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional   
        Shifts dataframe. The default is None.
    is_home : bool, optional
        Whether to get the home or away players. The default is True.

    '''
    html_shifts = fetch_html_shifts(game_id) if html_shifts is None else html_shifts
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters

    place = 'home' if is_home else 'away'

    df = html_shifts.copy().query("is_home==@is_home")
    
    # Create a time-based range
    game_duration = df['endTime_s'].max()  # Assumes the endTime_s column represents the game duration
    time_range = range(game_duration)  # +1 to include the last second

    # Create an empty list to store the sweater numbers per second
    playerId_per_second = []

    # Iterate through each second and collect sweater numbers
    for second in time_range:
        on_ice = df[(second >= df['startTime_s']) & (second < df['endTime_s'])
                    & (df['positionCode'].isin(['C', 'D', 'L', 'R']))] # Adjust position codes as needed
        playerId = list(set(on_ice['playerId'].tolist()))
        playerId_per_second.append(playerId)

    # Create a DataFrame with all seconds in the game
    time_df = pd.DataFrame({'Second': time_range})

    time_df[f'{place}Players'] = playerId_per_second

    time_df["game_id"] = game_id


    # Print the resulting list
    return time_df

def players_toi_per_strength(game_id=2023020005, game_rosters: Union[pd.DataFrame, None] = None, html_shifts: Union[pd.DataFrame, None] = None, is_home=True):
    '''
    Get the TOI per player per strength for a given game.

    Parameters
    ----------
    game_id : int
        Game ID to scrape.
    game_rosters : Union[pd.DataFrame, None], optional
        Game rosters dataframe. The default is None.
    html_shifts : Union[pd.DataFrame, None], optional
        Shifts dataframe. The default is None.
    is_home : bool, optional
        Whether to get the home or away players. The default is True.
    '''

    html_shifts = fetch_html_shifts(game_id) if html_shifts is None else html_shifts
    game_rosters = fetch_game_rosters(game_id) if game_rosters is None else game_rosters
    
    place = 'home' if is_home else 'away'
    not_place = 'away' if is_home else 'home'

    

    df = get_player_ids_per_second(game_id, game_rosters=game_rosters, html_shifts=html_shifts, is_home=is_home)
    df = df.explode(f'{place}Players')

    home_player_counts = get_player_count_per_second(game_id, html_shifts=html_shifts, is_home=is_home) #Gotta work on a fix with game_rosters
    away_player_counts = get_player_count_per_second(game_id, html_shifts=html_shifts, is_home=(not is_home)) #Gotta work on a fix with game_rosters

    df = df.merge(home_player_counts.merge(away_player_counts, on=["Second", "game_id"], how="left"), on=["Second", "game_id"], how="left")

    df['is_home2'] = 1 if is_home else 0
    df['strength'] = df.apply(lambda row: f'{row[f"{place}Count"]}v{row[f"{not_place}Count"]}', axis=1)

    result = (df.groupby([f'{place}Players', 'strength'], as_index=False)
                .size()
                .rename(columns={f'{place}Players': 'playerId',
                                 'size': 'Seconds'}))
    
    result = result.merge(game_rosters.query("is_home==@is_home"), on="playerId", how="left")
    result['strength'] = result['strength'].astype(str).str.replace('.0', '')

    # df2 = get_player_count_per_second(game_id=game_id, game_rosters=game_rosters,html_shifts=html_shifts,is_home=True).merge( get_player_count_per_second(game_id=game_id,html_shifts=html_shifts,is_home=False),  on=["Second", "game_id"], how="left")


    return result

