import pandas as pd
import numpy as np
import functools
import time

### MAKE DECORATORS WORK TOGETHER ###


def format_fetch_teams(func):
    """
    Wrapper function to format the output of fetch_teams_json.
    """
    
    @functools.wraps(func)
    def formatter(*args, **kwargs):
        df = pd.json_normalize(func(*args, **kwargs).get("teams", []))
        df.columns = df.columns.str.replace('.', '_')
        return df
    
    return formatter

def format_fetch_team_schedule(func):
    """
    Wrapper function to format the output of fetch_team_schedule_json.
    """
    
    @functools.wraps(func)
    def formatter(*args, **kwargs):
        df = pd.json_normalize(func(*args, **kwargs).get("games", []))
        
        df = df.rename(columns={'id': 'game_id',
                                'awayTeam.abbrev' : 'away_abbrev',
                                'homeTeam.abbrev' : 'home_abbrev',
                                'awayTeam.id' : 'away_id',
                                'homeTeam.id' : 'home_id',
                                'awayTeam.placeName.default' : 'away_placeName',
                                'homeTeam.placeName.default' : 'home_placeName',
                                'homeTeam.score' : 'home_score',
                                'awayTeam.score' : 'away_score',
                                'venue.default' : 'venue',
                                'periodDescriptor.periodType' : 'end'
                                })
        
        df.columns = df.columns.str.replace('.', '_')
        df[['game_id', 'season', 'away_id', 'home_id', 'home_score', 'away_score']] = df[['game_id', 'season', 'away_id', 'home_id', 'home_score', 'away_score']].apply(pd.to_numeric)
        df['gameDate'] = pd.to_datetime(df['gameDate'])

        df['home_goals'] = df['home_score']
        df['away_goals'] = df['away_score']

        df.loc[(df['end'] == 'SO') & (df["home_goals"] > df['away_goals']), 'home_goals'] = df['home_score'] - 1
        df.loc[(df['end'] == 'SO') & (df["home_goals"] < df['away_goals']), 'away_goals'] = df['away_score'] - 1

        df = df[['game_id', 'season', 'gameDate', 'away_id', 'home_id', 'home_goals', 'away_goals', 'home_score', 'away_score', 'end', 'venue', 'away_placeName', 'home_placeName']]
        return df
    
    return formatter

def timer(func):
    """
    Wrapper function to time the execution of a function.
    """
    
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    
    return wrapper_timer