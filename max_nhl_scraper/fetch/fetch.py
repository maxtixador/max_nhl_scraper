from constants import *
from helpers import *
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime 
import warnings
from typing import Dict, Union

warnings.filterwarnings('ignore')


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

