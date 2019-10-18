import os
import unittest
from queue import Queue

from cool_carbine import results_worker_wrapper, results_worker
from domain.http_consts import ContentTypes
from core.url_extract import UrlExtract, extract_urls
from core.url_parse import CCUrl, filter_url, parse_extracted_url, parse_extracted_url_list
from domain import SessionPair, SessionPairResultsDto, HttpClientResponseDto
from tests import async_test


class TestUrlParse(unittest.TestCase):
    @async_test
    async def test_extract_url(self):
        expected_length = 12

        urls = await self.get_extracted_urls('./test_data/html/angularjs_00001.html')
        actual_length = len(urls)

        self.assertEqual(expected_length, actual_length)

    @async_test
    async def test_long_parse(self):
        spr = await self.get_session_pair_results('./test_data/html/long_parse.html')
        queue = Queue()
        queue.put(spr)

        # await results_worker(queue, 1)

    def test_parse_url(self):
        base_url = CCUrl('https://test/com')

        expected_value = 0
        actual_value = len(parse_extracted_url_list(base_url, ["{{'global.menu.external.protectMyChoicesLink' | translate}}"], 0))

        self.assertEqual(expected_value, actual_value)

    def test_parse_url_none(self):
        base_url = CCUrl('https://test/com')
        expected_value = 0

        actual_value = len(parse_extracted_url_list(base_url, [None], 0))
        self.assertEqual(expected_value, actual_value)

        actual_value = len(parse_extracted_url_list(base_url, [''], 0))
        self.assertEqual(expected_value, actual_value)

        actual_value = len(parse_extracted_url_list(base_url, ['   '], 0))
        self.assertEqual(expected_value, actual_value)

    def test_parse_url_strip(self):
        base_url = CCUrl('https://test.com')

        expected_value = 1

        actual_value = len(parse_extracted_url_list(base_url, ['./asd '], 0))
        self.assertEqual(expected_value, actual_value)

        actual_value = len(parse_extracted_url_list(base_url, ['./asd\t'], 0))
        self.assertEqual(expected_value, actual_value)

        actual_value = len(parse_extracted_url_list(base_url, ['       ./asd\t'], 0))
        self.assertEqual(expected_value, actual_value)

        actual_value = len(parse_extracted_url_list(base_url, ['\t\n\r./asd\t'], 0))
        self.assertEqual(expected_value, actual_value)

    def test_parse_url_protocol_relative(self):
        expected_value = False
        actual_value = True

        base_url = CCUrl('https://test.com')
        test_url = CCUrl('//www.aftenbladet.no/trafikk/i/mqEEp/Webkameraer#101811')

        parsed_url = parse_extracted_url(base_url, test_url.url)
        actual_value = parsed_url.is_protocol_relative()

        self.assertEqual(expected_value, actual_value)

        expected_value = True
        actual_value = parsed_url.url.startswith(base_url.urlparse.scheme)

        self.assertEqual(expected_value, actual_value)

    async def get_extracted_urls(self, file_name: str):
        session_pair_results = await self.get_session_pair_results(file_name)

        return await extract_urls(session_pair_results, 0)

    async def get_session_pair_results(self, file_name: str):
        # TEXT_HTML
        hcrd = HttpClientResponseDto()
        hcrd.charset = 'utf-8'
        hcrd.content_type = ContentTypes.TEXT_HTML
        hcrd.status = 200
        hcrd.reason = 'OK'
        sprd = SessionPairResultsDto(
            SessionPair(None, 'https://test.com'),
            hcrd,
            await self.get_test_data(file_name)
        )

        return sprd

    async def get_test_data(self, file_name: str):
        with open(file_name, 'r') as fd:
            return fd.read()
