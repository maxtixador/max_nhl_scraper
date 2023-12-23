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