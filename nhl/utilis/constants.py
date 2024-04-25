#Constants
NHL_API_BASE_URL_1 = 'https://api-web.nhle.com/v1'

PLAY_BY_PLAY_ENDPOINT = f'{NHL_API_BASE_URL_1}/gamecenter/{{game_id}}/play-by-play'

SCHEDULE_ENDPOINT = f'{NHL_API_BASE_URL_1}/club-schedule-season/{{team}}/{{season}}'

SHIFT_REPORT_HOME_ENDPOINT = 'http://www.nhl.com/scores/htmlreports/{season}/TH{game_id}.HTM'
SHIFT_REPORT_AWAY_ENDPOINT = 'http://www.nhl.com/scores/htmlreports/{season}/TV{game_id}.HTM'

SHIFT_API_ENDPOINT = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={{game_id}}"


DEFAULT_SEASON = 20232024
DEFAULT_TEAM = "MTL"

DEFAULT_GAME_ID = 2023020464

SCHEDULE_CALENDAR_ENDPOINT = f'{NHL_API_BASE_URL_1}/schedule-calendar/{{date}}'

SCHEDULE_WEEK_ENDPOINT = f'{NHL_API_BASE_URL_1}/schedule/{{date}}'


STANDINGS_ENDPOINT = f'{NHL_API_BASE_URL_1}/standings/{{date}}'

