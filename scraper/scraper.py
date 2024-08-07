import sys
import os
import json
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
import numpy as np
import matplotlib.pyplot as plt
import glob
import re




from utilis.constants import *
from utilis.decorators import *
from utilis.functions import *

pd.set_option('display.max_columns', None)




# @timer
def scrape_game(game_id : int, file : str = None, save : bool = False,):
    """
    Scrape game data from NHL API

    Parameters
    ----------
    game_id : int
        Game ID
    file : str, optional
        File name to save data to, by default None
    save : bool, optional
        Save data to file, by default False

    Returns
    -------
    dict
        Dictionary of dataframes
    """

    
    shifts = fetch_html_shifts(game_id=game_id)

    # print(f"Fetching play-by-play for {game_id} \n")
    game_dict = fetch_play_by_play_json(game_id)

    rosters = pd.json_normalize(game_dict.get("rosterSpots", [])).set_index("playerId").assign(fullName = lambda x: x["firstName.default"] + " " + x["lastName.default"]).rename({"firstName.default": "firstName",
                                                                                                                                                                                  "lastName.default" : "lastName"}, axis=1)



    
    df = pd.json_normalize(game_dict.get("plays", []))

    # Add columns if they don't exist
    cols_to_add3 = ['details.winningPlayerId', 'details.losingPlayerId', 'details.hittingPlayerId',
                            'details.shootingPlayerId', 'details.scoringPlayerId', 'details.committedByPlayerId',
                            'details.blockingPlayerId','details.hitteePlayerId', 'details.assist1PlayerId',
                            'details.drawnByPlayerId', 'details.assist2PlayerId',
                            'details.eventOwnerTeamId', 'details.scoringPlayerTotal', 'details.assist1PlayerTotal',
                            'details.assist2PlayerTotal', 'situationCode', 'details.playerId', 'typeCode']
    
    for col in cols_to_add3:
        if col not in df.columns:
            df[col] = np.nan
    

    # Add game_id column
    df['gameId'], rosters['gameId'], shifts['gameId'] = game_id, game_id, game_id
    df['seasonId'] = game_dict.get('season', "")
    df['gameDate'], rosters['gameDate'], shifts['gameDate'] = pd.to_datetime(game_dict.get('gameDate', ""), format="%Y-%m-%d"), pd.to_datetime(game_dict.get('gameDate', ""), format="%Y-%m-%d"), pd.to_datetime(game_dict.get('gameDate', ""), format="%Y-%m-%d")
    df['gameType'] = game_dict.get('gameType', "")
    df['venue'] = game_dict.get('venue', "").get('default', "")

    df = df.rename(columns={'typeDescKey': 'event'})

    # Add period column ### New because of API Changes
    df['period'] = pd.to_numeric(df['periodDescriptor.number'], errors='coerce')

    # Add elapsed time column
    df['elapsedTime'] = (df['period'].astype(int) - 1) * 1200 + df['timeInPeriod'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1]))

    # Fill for missing scores
    df[['details.awayScore', 'details.homeScore', 'details.awaySOG', 'details.homeSOG']] = df[['details.awayScore', 'details.homeScore', 'details.awaySOG', 'details.homeSOG']].ffill().fillna(0)

    # Add normalized x-coordinate so that the home team is always defending the left side of the ice for future analysis

    
    # Add team abbreviations for each event
    df['eventTeam'] = df['details.eventOwnerTeamId'].map({game_dict.get('homeTeam', {}).get('id', "") : game_dict.get('homeTeam', {}).get('abbrev', ""),
                                                          game_dict.get('awayTeam', {}).get('id', "") : game_dict.get('awayTeam', {}).get('abbrev', "")})
    
    # Add home/away indicator
    df.loc[df['eventTeam'].notnull(),'is_home'] = (df['eventTeam'] == game_dict.get('homeTeam', {}).get('abbrev', "")).astype(int)

    df['event_player1_Id'] = df[['details.winningPlayerId', 'details.hittingPlayerId', 'details.shootingPlayerId', 'details.scoringPlayerId', 'details.committedByPlayerId', 'details.playerId']].bfill(axis=1).iloc[:, 0]

    df['event_player2_Id'] = df[['details.losingPlayerId',  'details.blockingPlayerId','details.hitteePlayerId', 'details.assist1PlayerId', 'details.drawnByPlayerId']].bfill(axis=1).iloc[:, 0]

    df['event_player3_Id'] = df['details.assist2PlayerId']

    df = df.drop(columns=cols_to_add3)

    #Make unique id for event
    df['uniqueId'] = pd.to_numeric(df['eventId'].astype(str) + df['gameId'].astype(str) + df['period'].astype(str) + df['sortOrder'].astype(str), errors='coerce')
    df = df.sort_values(by=['elapsedTime'], ascending=True).reset_index(drop=True)

    df = df.rename(columns={'periodDescriptor.number': 'periodDescriptor_number'})


    # Remove 'details.' and 'periodDescriptor.' prefixes from column names
    df.columns = df.columns.str.replace('details.', '').str.replace('periodDescriptor.', '')

    rosters['is_home'] = (rosters['teamId'] == game_dict.get('homeTeam', {}).get('id', "")).astype(int)

    shifts = shifts.merge(rosters.reset_index()[['is_home', 'sweaterNumber', 'playerId', 'positionCode']], how='left', on=['is_home', 'sweaterNumber'])

    home_sktrs_id = [shifts.query("positionCode != 'G' and startTime_s <= @second and endTime_s > @second and is_home == 1").playerId.unique().tolist() for second in df['elapsedTime']]
    n_home_sktrs = [shifts.query("positionCode != 'G' and startTime_s <= @second and endTime_s > @second and is_home == 1").playerId.nunique() for second in df['elapsedTime']]
    home_goalie_id = [shifts.query("positionCode == 'G' and startTime_s <= @second and endTime_s > @second and is_home == 1").playerId.unique().tolist()[0] if len(shifts.query("positionCode == 'G' and startTime_s <= @second and endTime_s > @second and is_home == 1").playerId.unique().tolist()) == 1 else np.nan for second in df['elapsedTime']]

    away_sktrs_id = [shifts.query("positionCode != 'G' and startTime_s <= @second and endTime_s > @second and is_home == 0").playerId.unique().tolist() for second in df['elapsedTime']]
    n_away_sktrs = [shifts.query("positionCode != 'G' and startTime_s <= @second and endTime_s > @second and is_home == 0").playerId.nunique() for second in df['elapsedTime']]
    away_goalie_id = [shifts.query("positionCode == 'G' and startTime_s <= @second and endTime_s > @second and is_home == 0").playerId.unique().tolist()[0] if len(shifts.query("positionCode == 'G' and startTime_s <= @second and endTime_s > @second and is_home == 0").playerId.unique().tolist()) == 1 else np.nan for second in df['elapsedTime']]

    # df['home_goalie'] = home_goalie_id
    # df['away_goalie'] = away_goalie_id

    df['home_skaters'] = n_home_sktrs
    df['away_skaters'] = n_away_sktrs

    df['game_strength'] = df.apply(lambda row: f"{row['home_skaters']}v{row['away_skaters']}" if row['is_home'] else f"{row['away_skaters']}v{row['home_skaters']}", axis=1)

    # Determine the maximum column index used in both home and away skater IDs
    max_column_index = max(
    max(len(home_skater_ids), len(away_skater_ids))
    for home_skater_ids, away_skater_ids in zip(home_sktrs_id, away_sktrs_id)
    )

    # Define column names for skater IDs and full names
    columns_to_add = [f"home_skater_id{j+1}" for j in range(max_column_index)]
    columns_to_add.extend([f"away_skater_id{j+1}" for j in range(max_column_index)])
    columns_to_add2 = [f"home_skater_fullName{j+1}" for j in range(max_column_index)]
    columns_to_add2.extend([f"away_skater_fullName{j+1}" for j in range(max_column_index)])

    # Check and add columns if they don't exist
    for column in columns_to_add + columns_to_add2:
        if column not in df.columns:
            df[column] = 'NaN'

    id_name_dict = rosters['fullName'].to_dict()
    # Assign values to the DataFrame for skater IDs and full names
    for i, (home_skater_ids, away_skater_ids) in enumerate(zip(home_sktrs_id, away_sktrs_id)):
        for j in range(max_column_index):
            if j < len(home_skater_ids):
                df.at[i, f"home_skater_id{j+1}"] = home_skater_ids[j]
                df.at[i, f"home_skater_fullName{j+1}"] = str(id_name_dict.get(home_skater_ids[j], ""))
            if j < len(away_skater_ids):
                df.at[i, f"away_skater_id{j+1}"] = away_skater_ids[j]
                df.at[i, f"away_skater_fullName{j+1}"] = str(id_name_dict.get(away_skater_ids[j], ""))

    df['event_player1_fullName'] = df['event_player1_Id'].map(id_name_dict)
    df['event_player2_fullName'] = df['event_player2_Id'].map(id_name_dict)
    df['event_player3_fullName'] = df['event_player3_Id'].map(id_name_dict)

    df = df.replace('NaN', np.nan)

    df['home_goalie_id'] = home_goalie_id
    df['away_goalie_id'] = away_goalie_id

    df['home_goalie_fullName'] = df['home_goalie_id'].map(id_name_dict)
    df['away_goalie_fullName'] = df['away_goalie_id'].map(id_name_dict)

    # Initialize 'normalized_xCoord' with 'xCoord'
    df['normalized_xCoord'] = df['xCoord']

    # Update 'normalized_xCoord' based on conditions
    df.loc[(df['homeTeamDefendingSide'] == 'right') & (df['is_home'] == 1), 'normalized_xCoord'] = df['xCoord'] * -1
    df.loc[(df['homeTeamDefendingSide'] == 'left') & (df['is_home'] == 0), 'normalized_xCoord'] = df['xCoord'] * -1

    # Calculate 'normalized_yCoord' based on 'normalized_xCoord'
    df['normalized_yCoord'] = np.where(df['normalized_xCoord'] == df['xCoord'], df['yCoord'], df['yCoord'] * -1)

    # Calculate 'normalized_xCoord_vertical' and 'normalized_yCoord_vertical' (offensive zone is on the top side of the ice rink)
    df['normalized_xCoord_vertical'] = -1 * df['normalized_yCoord']
    df['normalized_yCoord_vertical'] = df['normalized_xCoord']

    # print(rosters['fullName'].to_dict())
                
    # print(df.columns)
    rosters =  rosters.reset_index().rename(columns={'index': 'playerId'})
    
    # Define a regular expression pattern to match columns
    pattern = r'^(firstName|lastName)\.\w{2}$'

    # Filter columns that match the pattern and drop them
    columns_to_remove = [col for col in rosters.columns if re.match(pattern, col)]
    rosters = rosters.drop(columns=columns_to_remove)

    
    data_dict = {
        'pbp': df,
        'rosters': rosters,
        'shifts': shifts
    }

    returning_data = data_dict.get(file, []) if file else data_dict

    
    # print(data_dict['rosters'].columns)

    if save:
        for key, value in data_dict.items():
            value.to_csv(f"{key}.csv", index=False)

    return returning_data



if __name__ == "__main__":
    scrape_game(2020020001, save=True)
    # scrape_game(2020020001, save=True)
   
