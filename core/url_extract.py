import time
from typing import List

from bs4 import BeautifulSoup
from structlog import get_logger

from core.url_parse import parse_url, parse_extracted_url_list, CCUrl
from domain import SessionPairResultsDto


log = get_logger()


def parse_html(session_pair_results: SessionPairResultsDto, worker_id: int) -> List[str]:
    bs_start = time.perf_counter()
    soup = BeautifulSoup(session_pair_results.response_body, 'html.parser')
    links = soup.find_all('a')
    bs_end = time.perf_counter()
    log.debug('BeautifulSoup perf_counter', results_worker=worker_id, start=bs_start, end=bs_end, elapsed=bs_end - bs_start, url=session_pair_results.url)

    return links


async def extract_urls(session_pair_results: SessionPairResultsDto, worker_id: int) -> List[CCUrl]:
    try:
        links = parse_html(session_pair_results, worker_id)

        parse_start = time.perf_counter()
        base_url = parse_url(session_pair_results.url)
        hrefs: List[str] = []
        for link in links:
            hrefs.append(link.get('href'))
        parsed = parse_extracted_url_list(base_url, hrefs, worker_id)
        parse_end = time.perf_counter()

        log.debug('URL parsing perf_counter', results_worker=worker_id, start=parse_start, end=parse_end,
                  elapsed=parse_end - parse_start, url=session_pair_results.url)

        return parsed
    except Exception as ex:
        pass  # log.exception('Unknown exception when extracting URLs.', results_worker=worker_id, url=session_pair_results.url)
        raise ex

    return []


class UrlExtract():
    async def next(self, session_pair_results: SessionPairResultsDto, worker_id: int) -> List[CCUrl]:
        pass