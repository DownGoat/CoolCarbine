

DATABASE_CONFIG = {
    'user': 'strix',
    'password': 'strix',
    'database': 'strix',
    'host': '172.17.0.2'
}

MAX_HOURLY_VISITS = 10

PARSE_CONFIG = {
    'core.url_extract': {}
}


HTTP_CONFIG = (
    'aiohttp', {
        'headers': {
            'User-Agent': 'Mozilla/5.0 (compatible; CoolCarbine/0.1-dev; +http://www.puse.cat/bot.html)'
        }
    })

LOGGING = {
    'LOG_LEVEL': 'DEBUG'
}

