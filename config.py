DATABASE_CONFIG = {
    'user': 'root',
    'password': '123qwe',
    'database': 'cool_carbine',
    'host': '172.17.0.4'
}

MAX_HOURLY_VISITS = 10

RESULTS_CONFIG = {
    'workers': 1
}

PARSE_CONFIG = {
    'core.url_extract': {}
}

RECORDER_CONFIG = {
    'enable_page_recorder': False
}

HTTP_CONFIG = {
    'workers': 1,
    'worker': {
        'name': 'aiohttp',
        'timeout': 15,
        'headers': {
            'User-Agent': 'Mozilla/5.0 (compatible; CoolCarbine/0.1-dev; +http://www.puse.cat/bot.html)'
        }
    }
}

LOGGING = {
    'LOG_LEVEL': 'DEBUG'
}
