import unittest

from core.database import get_connection
from tests import async_test


class TestQueue(unittest.TestCase):
    @async_test
    async def test_get_path(self):
        pass
