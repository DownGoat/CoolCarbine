import unittest

from core.database import get_connection
from tests import async_test


class TestQueue(unittest.TestCase):
    @async_test
    async def test_get_path(self):
        connection = await get_connection()
        values = await connection.fetch(
            '''select distinct on(netloc) netloc, scheduled from queue where netloc = any($1::varchar[]) order by netloc, scheduled desc;''',
            ['abo.schibsted.no', 'aksjelive.e24.no', '2016.javazone.no']
        )

        latest_schedule = dict()
        for v in values:
            latest_schedule[v.get('netloc')] = v.get('scheduled')
        await connection.close()
