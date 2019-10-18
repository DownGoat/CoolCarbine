import os
import shutil
import unittest

from core.store import get_path, STORAGE_DIRECTORY, path_contains_file, make_file_index
from domain import SessionPairResults, SessionPair


class TestStore(unittest.TestCase):

    def test_get_path(self):
        session_pair_results = self.get_session_pair_results('https://kundeportal.vg.no/minside')
        path, file_name = get_path(session_pair_results)

        self.assertEqual(
            'd7f70b78934eda8cf95d243f712c936bf5e4b921b1085f442ee5cc3a0469bcbf',
            path
        )

        self.assertEqual(
            'cd05fc60031fb51603c2ae1f6449709d657111aed9e057196d6e02f6cb3a730e',
            file_name
        )

    def test_get_path_folder_index_url(self):
        session_pair_results = self.get_session_pair_results('https://kundeportal.vg.no/minside/')
        path, file_name = get_path(session_pair_results)

        self.assertEqual(
            'd7f70b78934eda8cf95d243f712c936bf5e4b921b1085f442ee5cc3a0469bcbf/cd05fc60031fb51603c2ae1f6449709d657111aed9e057196d6e02f6cb3a730e',
            path
        )

        self.assertEqual(
            '0eb547304658805aad788d320f10bf1f292797b5e6d745a3bf617584da017051',
            file_name
        )

    def test_get_path_index_url(self):
        session_pair_results = self.get_session_pair_results('https://kundeportal.vg.no/')
        path, file_name = get_path(session_pair_results)

        self.assertEqual(
            'd7f70b78934eda8cf95d243f712c936bf5e4b921b1085f442ee5cc3a0469bcbf',
            path
        )

        self.assertEqual(
            '0eb547304658805aad788d320f10bf1f292797b5e6d745a3bf617584da017051',
            file_name
        )

        session_pair_results = self.get_session_pair_results('https://kundeportal.vg.no')
        path, file_name = get_path(session_pair_results)

        self.assertEqual(
            'd7f70b78934eda8cf95d243f712c936bf5e4b921b1085f442ee5cc3a0469bcbf',
            path
        )

        self.assertEqual(
            '0eb547304658805aad788d320f10bf1f292797b5e6d745a3bf617584da017051',
            file_name
        )

    def test_path_contains_file(self):
        expected_contains = True
        expected_path = 'a/file'

        self.create_file(
            STORAGE_DIRECTORY,
            'a',
            'file'
        )

        actual_contains, actual_path = path_contains_file(
            expected_path
        )

        self.assertEqual(expected_contains, actual_contains)
        self.assertEqual(expected_path, actual_path)

        self.delete_directory(
            STORAGE_DIRECTORY,
            'a',
        )

    def test_make_file_index(self):
        self.create_file(
            STORAGE_DIRECTORY,
            'a',
            'file'
        )

        self.assertEqual(True, os.path.isfile(os.path.join(STORAGE_DIRECTORY, 'a', 'file')))

        make_file_index('a', 'file')

        self.assertEqual(True, os.path.exists(os.path.join(STORAGE_DIRECTORY, 'a', 'file')))
        self.assertEqual(True, os.path.isdir(os.path.join(STORAGE_DIRECTORY, 'a', 'file')))

        self.delete_directory(
            STORAGE_DIRECTORY,
            'a',
        )

    @staticmethod
    def get_session_pair_results(url: str, data='foo', client=None):
        return SessionPairResults(
            SessionPair(
                client,
                url
            ),
            None,
            data
        )

    @staticmethod
    def create_file(base: str, path: str, file_name: str):
        splitted = os.path.split(path)

        current_path = ''
        for directory in splitted:
            current_path = os.path.join(current_path, directory)
            full_path = os.path.join(base, current_path)
            if not os.path.exists(full_path):
                os.mkdir(full_path)

        with open(os.path.join(base, current_path, file_name), 'w') as fd:
            fd.write('1')

    @staticmethod
    def delete_directory(base: str, path: str):
        root = path.split(os.path.sep)[0]
        delete_path = os.path.join(base, root)
        shutil.rmtree(delete_path)
