import asyncpg


async def get_connection():
    return await asyncpg.connect(user='strix', password='strix',
                                 database='cool_carbine', host='172.17.0.2')
