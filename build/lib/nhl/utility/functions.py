import requests
from .constants import *

# import json
from .helpers import *
import pandas as pd
import numpy as np
from .decorators import *
from datetime import datetime 
from bs4 import BeautifulSoup

# @timer
def fetch_teams_json(date = "now"):
    """
    Connects to the NHL API to get the data for a given date.

    Args:
      date: Date in the format of YYYY-MM-DD.

    Returns:
      A JSON file with the information of the games on a given date.

    Raises:
      requests.exceptions.RequestException: If there's an issue with the request.
    """



    date = date if is_valid_date(date) else "now"
    response = requests.get(SCHEDULE_CALENDAR_ENDPOINT.format(date=date))
    response.raise_for_status()
    return response.json()

# @timer
def fetch_play_by_play_json(game_id: int):
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

# @timer
def fetch_team_schedule_json(team: str = DEFAULT_TEAM, season: int = DEFAULT_SEASON):
    """
    Connects to the NHL API to get the data for a given team's schedule.

    Args:
      team : Team abbreviation.
      season: Desired season in the format of {year_start}{year_end}.

    Returns:
      A JSON file with the schedule of a given team.

    Raises:
      requests.exceptions.RequestException: If there's an issue with the request.
    """


    # Prevent errors if the user passes in a team name instead of an abbreviation.
    team_var = find_key_by_value_in_list((pd.json_normalize(fetch_teams_json(f"{str(season)[4::]}-01-01").get("teams", []))
    .set_index("abbrev").iloc[:, 6:]
    .reset_index()
    .assign(abbreviation = lambda x : x.abbrev)
    .set_index("abbrev")
    .replace({"NaN" : np.nan})
    .assign(names=lambda x: x.apply(lambda row: row.dropna().tolist(), axis=1))
    .filter([ "names"])
    .to_dict(orient="series")
    .get("names")
    .to_dict()), team)


    response = requests.get(SCHEDULE_ENDPOINT.format(team=team_var, season=season))
    response.raise_for_status()
    return response.json()

# @timer
def fetch_schedule_week_json(date = "now"):
    """
    Connects to the NHL API to get the data for a given date.

    Args:
      date: Date in the format of YYYY-MM-DD.

    Returns:
      A JSON file with the information of the games on a given date.

    Raises:
      requests.exceptions.RequestException: If there's an issue with the request.
    """
    date = date if is_valid_date(date) else "now"
    response = requests.get(SCHEDULE_WEEK_ENDPOINT.format(date=date))
    response.raise_for_status()
    return response.json()

# @timer
def fetch_standings_json(date = "now"):

    """
    Connects to the NHL API to get the standings for a given date.

    Args:
      date: Date in the format of YYYY-MM-DD.

    Returns:
      A JSON file with the standings for a given date.

    Raises:
      requests.exceptions.RequestException: If there's an issue with the request.
    """
    date = date if is_valid_date(date) else "now"
    response = requests.get(STANDINGS_ENDPOINT.format(date=date))
    response.raise_for_status()
    return response.json()

# @timer
def get_teams(date = "now"):
    """
    Gets the teams for a given date.

    Args:
      date: Date in the format of YYYY-MM-DD.

    Returns:
      A dataframe with the teams for a given date.

    Raises:
      requests.exceptions.RequestException: If there's an issue with the request.
    """

    date_dummy = datetime.now().strftime("%Y-%m-%d") if date == "now" else date

    print(f"Fetching teams for {date_dummy} \n")

    df = pd.json_normalize(fetch_teams_json(date).get("teams", []))
    df.columns = df.columns.str.replace('.', '_')
    df = df.rename(columns={'id': 'teamId'})
    
    return df

# @timer
def get_team_schedule(team: str = DEFAULT_TEAM, season: int = DEFAULT_SEASON):
    """
    Gets the schedule for a given team.

    Args:
        team : Team abbreviation.
        season: Desired season in the format of {year_start}{year_end}.

    Returns:
        A dataframe with the schedule for a given team.

    Raises:
        requests.exceptions.RequestException: If there's an issue with the request.
    """

    print(f"Fetching schedule for {team} in {season} \n")
    df = pd.json_normalize(fetch_team_schedule_json(team, season).get("games", []))

    df = df.rename(columns={'id': 'gameId'})
    df.columns = df.columns.str.replace('.', '_')

    df.loc[:, 'awayTeam_goals'] = df['awayTeam_score']
    df.loc[:, 'homeTeam_goals'] = df['homeTeam_score']

    df.loc[(df['periodDescriptor_periodType'] == 'SO') & (df["homeTeam_goals"] > df['awayTeam_goals']), 'homeTeam_goals'] = df['homeTeam_score'] - 1
    df.loc[(df['periodDescriptor_periodType'] == 'SO') & (df["homeTeam_goals"] < df['awayTeam_goals']), 'awayTeam_goals'] = df['awayTeam_score'] - 1


    df.loc[df['homeTeam_score'] > df['awayTeam_score'], 'winner_abbrev'] = df['homeTeam_abbrev']
    df.loc[df['homeTeam_score'] < df['awayTeam_score'], 'winner_abbrev'] = df['awayTeam_abbrev']

    df.loc[df['homeTeam_score'] > df['awayTeam_score'], 'loser_abbrev'] = df['awayTeam_abbrev']
    df.loc[df['homeTeam_score'] < df['awayTeam_score'], 'loser_abbrev'] = df['homeTeam_abbrev']

    df = df.fillna(np.nan)
                            
    return df

# @timer
def get_schedule_week(date = "now"):
    """
    Gets the schedule for a given week.

    Args:
        date: Date in the format of YYYY-MM-DD.

    Returns:
        A dataframe with the schedule for a given week.

    Raises:
        requests.exceptions.RequestException: If there's an issue with the request.
    """

    print(f"Fetching schedule for week of {date} \n")
    df = pd.json_normalize(fetch_schedule_week_json(date).get("gameWeek", {}))

    df = pd.concat([pd.json_normalize(game) for game in df['games']], ignore_index=True)

    df = df.rename(columns={'id': 'gameId'})
    df.columns = df.columns.str.replace('.', '_')

    df.loc[:, 'awayTeam_goals'] = df['awayTeam_score']
    df.loc[:, 'homeTeam_goals'] = df['homeTeam_score']

    df.loc[(df['periodDescriptor_periodType'] == 'SO') & (df["homeTeam_goals"] > df['awayTeam_goals']), 'homeTeam_goals'] = df['homeTeam_score'] - 1
    df.loc[(df['periodDescriptor_periodType'] == 'SO') & (df["homeTeam_goals"] < df['awayTeam_goals']), 'awayTeam_goals'] = df['awayTeam_score'] - 1


    df.loc[df['homeTeam_score'] > df['awayTeam_score'], 'winner_abbrev'] = df['homeTeam_abbrev']
    df.loc[df['homeTeam_score'] < df['awayTeam_score'], 'winner_abbrev'] = df['awayTeam_abbrev']

    df.loc[df['homeTeam_score'] > df['awayTeam_score'], 'loser_abbrev'] = df['awayTeam_abbrev']
    df.loc[df['homeTeam_score'] < df['awayTeam_score'], 'loser_abbrev'] = df['homeTeam_abbrev']

    df = df.fillna(np.nan)
                            
    return df

# @timer
def get_standings(date = "now"):
    """
    Gets the standings for a given date.

    Args:
        date: Date in the format of YYYY-MM-DD.

    Returns:
        A dataframe with the standings for a given date.

    Raises:
        requests.exceptions.RequestException: If there's an issue with the request.
    """

    print(f"Fetching standings for {date} \n")
    df = pd.json_normalize(fetch_standings_json(date).get("standings", []))

    # df = pd.concat([pd.json_normalize(record) for record in df['teamRecords']], ignore_index=True)

    df.columns = df.columns.str.replace('.', '_')

                            
    return df

# @timer
def get_pbp(game_id: int):
    """
    Gets the play-by-play for a given game.

    Args:
        game_id: Identifier ID for a given game.

    Returns:
        A dataframe with the play-by-play for a given game.

    Raises:
        requests.exceptions.RequestException: If there's an issue with the request.
    """

    print(f"Fetching play-by-play for {game_id} \n")

    game_dict = fetch_play_by_play_json(game_id)
    
    df = pd.json_normalize(game_dict.get("plays", []))

    # Add game_id column
    df['gameId'] = game_id
    df['seasonId'] = game_dict.get('season', "")
    df['gameDate'] = pd.to_datetime(game_dict.get('gameDate', ""), format="%Y-%m-%d")
    df['gameType'] = game_dict.get('gameType', "")
    df['venue'] = game_dict.get('venue', "").get('default', "")

    df = df.rename(columns={'typeDescKey': 'event'})

    # Add elapsed time column
    df['elapsedTime'] = (df['period'].astype(int) - 1) * 1200 + df['timeInPeriod'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1]))

    # Fill for missing scores
    df[['details.awayScore', 'details.homeScore', 'details.awaySOG', 'details.homeSOG']] = df[['details.awayScore', 'details.homeScore', 'details.awaySOG', 'details.homeSOG']].ffill().fillna(0)

    # Add normalized x-coordinate so that the home team is always defending the left side of the ice for future analysis
    df['normalized_xCoord'] = df.apply(adjust_x_coord, axis=1)



    
    # Add team abbreviations for each event
    df['eventTeam'] = df['details.eventOwnerTeamId'].map({game_dict.get('homeTeam', {}).get('id', "") : game_dict.get('homeTeam', {}).get('abbrev', ""),
                                                          game_dict.get('awayTeam', {}).get('id', "") : game_dict.get('awayTeam', {}).get('abbrev', "")})
    
    # Add home/away indicator
    df.loc[df['eventTeam'].notnull(),'is_home'] = (df['eventTeam'] == game_dict.get('homeTeam', {}).get('abbrev', "")).astype(int)

    df['event_player1_Id'] = df[['details.winningPlayerId', 'details.hittingPlayerId', 'details.shootingPlayerId', 'details.scoringPlayerId', 'details.committedByPlayerId', 'details.playerId']].bfill(axis=1).iloc[:, 0]

    df['event_player2_Id'] = df[['details.losingPlayerId',  'details.blockingPlayerId','details.hitteePlayerId', 'details.assist1PlayerId', 'details.drawnByPlayerId']].bfill(axis=1).iloc[:, 0]

    df['event_player3_Id'] = df['details.assist2PlayerId']

    df = df.drop(columns=['details.winningPlayerId', 'details.losingPlayerId', 'details.hittingPlayerId',
                            'details.shootingPlayerId', 'details.scoringPlayerId', 'details.committedByPlayerId',
                            'details.blockingPlayerId','details.hitteePlayerId', 'details.assist1PlayerId',
                            'details.drawnByPlayerId', 'details.assist2PlayerId',
                            'details.eventOwnerTeamId', 'details.scoringPlayerTotal', 'details.assist1PlayerTotal',
                            'details.assist2PlayerTotal', 'situationCode','eventId', 'details.playerId'])

    df = df.sort_values(by=['elapsedTime'], ascending=True).reset_index(drop=True)

    df = df.rename(columns={'periodDescriptor.number': 'period'})


    # Remove 'details.' and 'periodDescriptor.' prefixes from column names
    df.columns = df.columns.str.replace('details.', '').str.replace('periodDescriptor.', '')

    # Now, the column names will be updated with the prefixes removed


    # #Fix score when goal
    # df.loc[(df['event'] == 'goal') & (df['is_home'] == 1), 'details.homeScore'] = df['details.homeScore'] + 1
    # df.loc[(df['event'] == 'goal') & (df['is_home'] == 0), 'details.awayScore'] = df['details.awayScore'] + 1



    return df

# @timer
def fetch_html_shifts(game_id=2023020069, season=None, pbp_json=None):
    ''' 
    Fetches shifts data from the NHL API and returns a DataFrame with the data.
    ----
    :param game_id: The game ID of the game to fetch shifts for.
    :param season: The season of the game. If not provided, it will be fetched from the API.
    :param pbp_json: The play-by-play JSON for the game. If not provided, it will be fetched from the API.
    :return: A DataFrame containing the shifts data for the game.
    '''

    # pbp_json = fetch_play_by_play_json(game_id) if pbp_json is None else pbp_json
    # rosters = fetch_game_rosters(game_id)

    season = f"{str(game_id)[:4]}{int(str(game_id)[:4]) + 1}" if season is None else season


    ### HOME SHIFTS ###
    url = SHIFT_REPORT_HOME_ENDPOINT.format(season=season, game_id=str(game_id)[4:])
    page = (requests.get(url))
    soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'html.parser', multi_valued_attributes = None)
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
    soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'html.parser', multi_valued_attributes = None)
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
                  .drop(columns=['name', 'team']))
    #               .merge(rosters, how='left', on=['sweaterNumber', 'is_home']))


    all_shifts[['startTime', 'startTime_remaning']] = all_shifts['shift_start'].str.split(' / ', expand=True)

    # # Split 'shift_end' column into two columns
    all_shifts[['endTime', 'endTime_remaning']] = all_shifts['shift_end'].str.split(' / ', expand=True)    

    all_shifts = all_shifts.drop(columns=[ 'startTime_remaning',  'endTime_remaning', 'shift_start', 'shift_end']).replace({'OT':4})
    
    str_to_sec = lambda x: int(x.split(':')[0]) * 60 + int(x.split(':')[1])

    all_shifts['period'] = all_shifts['period'].astype(int)
    all_shifts['duration_s'] = all_shifts['duration'].fillna('00:00').apply(str_to_sec)
    all_shifts['startTime_s'] = all_shifts['startTime'].apply(str_to_sec) + 60 * (all_shifts['period'] - 1) * 20
    all_shifts['endTime_s'] = all_shifts['endTime'].apply(str_to_sec) + 60 * (all_shifts['period'] - 1) * 20
    
    

    # all_shifts['date'] = pbp_json['gameDate']
    # all_shifts['season'] = pbp_json['season']
    # all_shifts['gameType'] = game_id


    return all_shifts

# /Users/max/Documents/Projects/max_nhl_scraper/.venv/bin/python -m nhl.utility.functions

# print(fetch_html_shifts(game_id=2023020069))