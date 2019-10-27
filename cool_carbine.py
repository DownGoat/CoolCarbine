import asyncio
import multiprocessing
import time
from multiprocessing import Process
from queue import Queue, Empty
from typing import List
from urllib.parse import urlparse, urlsplit

from structlog import get_logger

from config import HTTP_CONFIG, RESULTS_CONFIG, RECORDER_CONFIG
from core.cool_carbine_http import http_worker_wrapper
from core.database import get_connection
from core.page_recorder import record_page_connections
from core.queue import add_to_queue, queue_worker
from core.url_extract import extract_urls
from core.url_parse import CCUrl
from domain import http_consts, SessionPairResultsDto

MAX_HOURLY_VISITS = 20

log = get_logger()


async def record_visit(session_pair_results: SessionPairResultsDto, worker_id: int):
    if session_pair_results is None or not hasattr(session_pair_results, 'session_pair'):
        print(session_pair_results)

    connection = await get_connection()
    parsed_url = urlparse(session_pair_results.url)

    await connection.execute(
        '''insert into visits (netloc, url) values ($1, $2)''',
        parsed_url.netloc, session_pair_results.url)
    await connection.close()


async def create_git_urls(extracted_urls: List[CCUrl], worker_id=int) -> List[CCUrl]:
    try:
        netlocs = dict()
        for url in extracted_urls:
            if url.urlparse.netloc not in netlocs:
                netlocs[url.urlparse.netloc] = url.urlparse

        git_urls: List[CCUrl] = []
        for netloc, url in netlocs.items():
            url_parts = urlsplit(url.geturl())
            git_url = CCUrl(url_parts._replace(path='/.git/HEAD').geturl())
            git_urls.append(git_url)
            log.info('Creating git url for netloc.', netloc=netloc, results_worker=worker_id)

        return git_urls + extracted_urls
    except Exception as ex:
        log.exception('Something wen wrong when creating git url for netloc.', results_worker=worker_id)

    return []


async def insert_git_response(session_pair_results: SessionPairResultsDto, worker_id: int):
    connection = await get_connection()
    try:
        status = 'None'
        if session_pair_results.client_response and session_pair_results.client_response.redirected:
            session_pair_results.client_response.status = '3xx'
            status = session_pair_results.client_response.status
        elif session_pair_results.client_response and session_pair_results.client_response.content_type == http_consts.ContentTypes.TEXT_HTML:
            session_pair_results.client_response.status = '2html'
            status = session_pair_results.client_response.status
        elif session_pair_results.client_response:
            status = session_pair_results.client_response.status

        await connection.execute(
            '''insert into git_heads (url, status) values ($1, $2); ''',
            session_pair_results.url,
            str(status)
        )
    except Exception as ex:
        log.exception('Unknown error when creating git response record.', results_worker=worker_id, exception=str(type(ex)), exception_message=str(ex), url=session_pair_results.url)
    finally:
        await connection.close()


async def handle_response(session_pair_results: SessionPairResultsDto, worker_id: int):
    if session_pair_results.url.endswith('/.git/HEAD'):
        await insert_git_response(session_pair_results, worker_id)

    if session_pair_results.response_body is not None:
        # TODO Disable this.
        # await store_page(session_pair_results)
        if session_pair_results.client_response.content_type == http_consts.ContentTypes.TEXT_HTML:
            extracted_urls = await extract_urls(session_pair_results, worker_id)
            t1_start = time.perf_counter()
            if RECORDER_CONFIG.get('enable_page_recorder'):
                await record_page_connections(extracted_urls, session_pair_results, worker_id)
            else:
                log.debug('page recorder disabled.', results_worker=worker_id)
            t1_end = time.perf_counter()
            log.debug('record_page_connections perf_counter.', start=t1_start, end=t1_end, elapsed=t1_end - t1_start, url=session_pair_results.url, results_worker=worker_id)

            t2_start = time.perf_counter()
            extracted_urls = await create_git_urls(extracted_urls, worker_id)
            t2_end = time.perf_counter()
            log.debug('create_git_urls perf_counter.', start=t2_start, end=t2_end, elapsed=t2_end - t2_start, url=session_pair_results.url, results_worker=worker_id)

            t3_start = time.perf_counter()
            await add_to_queue(extracted_urls, worker_id)
            t3_end = time.perf_counter()
            log.debug('add_to_queue perf_counter.', start=t3_start, end=t3_end, elapsed=t3_end - t3_start, urls=len(extracted_urls), per_row=(t3_end - t3_start)/len(extracted_urls), url=session_pair_results.url, results_worker=worker_id)

        else:
            log.debug('Unhandled content-type', results_worker=worker_id, content_type=session_pair_results.client_response.content_type, url=session_pair_results.url)
    else:
        log.debug('There was no response for this request', session_pair_results=session_pair_results, results_worker=worker_id)


async def results_worker(results_queue: 'Queue[SessionPairResultsDto]', worker_id: int):
    log.info('Results worker starting.', results_worker=worker_id)

    while True:
        try:
            work = results_queue.get(timeout=1)
            log.debug(f'results_queue size', size=results_queue.qsize(), results_worker=worker_id)

            t1_start = time.perf_counter()
            await record_visit(work, worker_id)
            await handle_response(work, worker_id)
            t1_end = time.perf_counter()

            log.debug(f'perf_counter stats', elapsed=t1_end - t1_start, start=t1_start, end=t1_end, url=work.url, results_worker=worker_id)

        except Empty:
            await asyncio.sleep(10)
        except Exception as ex:
            log.exception('Unknown exception in ResultsWorker.', results_worker=worker_id,  exception=str(type(ex)), exception_message=str(ex))


def results_worker_wrapper(results_queue: 'Queue[SessionPairResultsDto]', worker_id: int):
    asyncio.run(results_worker(results_queue, worker_id))


async def start_workers(loop):
    queue = Queue()
    results_queue = multiprocessing.Queue()

    workers = [http_worker_wrapper(queue, results_queue, x) for x in range(HTTP_CONFIG.get('workers', 10))]
    workers = [queue_worker(queue)] + workers

    for x in range(RESULTS_CONFIG.get('workers', 12)):
        Process(target=results_worker_wrapper, args=(results_queue, x)).start()

    await asyncio.gather(*workers)


async def main(loop):
    await start_workers(loop)


async def testmain(loop):
    pass


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
