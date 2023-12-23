from constants import *
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")



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
