import asyncpg


async def get_connection():
    return await asyncpg.connect(user='root', password='123qwe',
                                 database='cool_carbine', host='172.17.0.3')
