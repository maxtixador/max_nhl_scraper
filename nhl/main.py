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


from utility.constants import *
from utility.decorators import *
from utility.functions import *
pd.set_option('display.max_columns', None)

@timer
def scrape_game(game_id : int, file : str = None, save : bool = False):
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

    print(f"Fetching play-by-play for {game_id} \n")
    game_dict = fetch_play_by_play_json(game_id)

    rosters = pd.json_normalize(game_dict.get("rosterSpots", [])).set_index("playerId").assign(fullName = lambda x: x["firstName.default"] + " " + x["lastName.default"]).drop(columns=["firstName.default", "lastName.default"])



    
    df = pd.json_normalize(game_dict.get("plays", []))

    # Add game_id column
    df['gameId'], rosters['gameId'], shifts['gameId'] = game_id, game_id, game_id
    df['seasonId'] = game_dict.get('season', "")
    df['gameDate'], rosters['gameDate'], shifts['gameDate'] = pd.to_datetime(game_dict.get('gameDate', ""), format="%Y-%m-%d"), pd.to_datetime(game_dict.get('gameDate', ""), format="%Y-%m-%d"), pd.to_datetime(game_dict.get('gameDate', ""), format="%Y-%m-%d")
    df['gameType'] = game_dict.get('gameType', "")
    df['venue'] = game_dict.get('venue', "").get('default', "")

    df = df.rename(columns={'typeDescKey': 'event'})

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

    df = df.drop(columns=['details.winningPlayerId', 'details.losingPlayerId', 'details.hittingPlayerId',
                            'details.shootingPlayerId', 'details.scoringPlayerId', 'details.committedByPlayerId',
                            'details.blockingPlayerId','details.hitteePlayerId', 'details.assist1PlayerId',
                            'details.drawnByPlayerId', 'details.assist2PlayerId',
                            'details.eventOwnerTeamId', 'details.scoringPlayerTotal', 'details.assist1PlayerTotal',
                            'details.assist2PlayerTotal', 'situationCode','eventId', 'details.playerId', 'typeCode'])

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

    # print(rosters['fullName'].to_dict())
                
    # print(df.columns)

    data_dict = {
        'pbp': df,
        'rosters': rosters,
        'shifts': shifts
    }

    returning_data = data_dict.get(file, []) if file else data_dict

    if save:
        for key, value in data_dict.items():
            value.to_csv(f"data/{key}/{key}_{game_id}.csv", index=False)
            

    return returning_data

### SCRAPE ALL HABS GAMES (PLAYED) AND SAVE TO CSV
# games_list = get_team_schedule(team="MTL", season = 20232024).query("gameType == 2 and gameState != 'FUT'").gameId.tolist()

    # get_pbp(game_id=game_id).to_csv(f"data/pbp_{game_id}.csv", index=False)
    # for i, game_id in enumerate(games_list):
    #     print(f"Scraping game {i+1}/{len(games_list)} \n ----------------------------------------- \n")
    #     scrape_game(game_id, save=True)

### RETRIEVE ALL GAMES FROM CSV
    # # Get a list of all CSV files in the 'pbp' directory
    # csv_files = glob.glob('data/pbp/*.csv')

    # # Initialize an empty list to store the dataframes
    # dfs = []

    # # Read each CSV file and append it to the list
    # for csv_file in csv_files:
    #     df = pd.read_csv(csv_file)
    #     dfs.append(df)

    # # Concatenate all dataframes in the list
    # combined_df = pd.concat(dfs, ignore_index=True).reset_index(drop=True)

    # print(combined_df)



if __name__ == "__main__":
    
    # date = datetime.now().strftime("%Y-%m-%d")
    # game_id = 2023020361

    games_list = get_team_schedule(team="MTL", season = 20232024).query("gameType == 2 and gameState != 'FUT'").gameId.tolist()

    # get_pbp(game_id=game_id).to_csv(f"data/pbp_{game_id}.csv", index=False)
    for i, game_id in enumerate(games_list):
        print(f"Scraping game {i+1}/{len(games_list)} \n ----------------------------------------- \n")
        scrape_game(game_id, save=True)



    # print(scrape_game(game_id, save=True))

    


    # print("Hello World!")