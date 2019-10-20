import asyncio
import multiprocessing
import time
from multiprocessing import Process
from queue import Queue, Empty
from urllib.parse import urlparse

import structlog
from structlog import get_logger

from core.cool_carbine_http import http_worker_wrapper
from core.database import get_connection
from core.page_recorder import record_page_connections
from core.queue import add_to_queue, queue_worker
from core.url_extract import UrlExtract, extract_urls
from domain import http_consts, SessionPairResultsDto


MAX_HOURLY_VISITS = 20

import structlog


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


async def handle_response(session_pair_results: SessionPairResultsDto, worker_id: int):
    if session_pair_results.response_body is not None:
        # TODO Disable this.
        # await store_page(session_pair_results)
        if session_pair_results.client_response.content_type == http_consts.ContentTypes.TEXT_HTML:
            extracted_urls = await extract_urls(session_pair_results, worker_id)
            t1_start = time.perf_counter()
            await record_page_connections(extracted_urls, session_pair_results, worker_id)
            t1_end = time.perf_counter()
            log.debug('record_page_connections perf_counter.', start=t1_start, end=t1_end, elapsed=t1_end - t1_start, url=session_pair_results.url)
            t2_start = time.perf_counter()
            await add_to_queue(extracted_urls, worker_id)
            t2_end = time.perf_counter()
            log.debug('add_to_queue perf_counter.', start=t2_start, end=t2_end, elapsed=t2_end - t2_start, url=session_pair_results.url)
        else:
            log.debug('Unhandled content-type', results_worker=worker_id, content_type=session_pair_results.client_response.content_type, url=session_pair_results.url)
    else:
        log.debug('There was no response for this request', session_pair_results=session_pair_results, results_worker=worker_id)


async def results_worker(results_queue: 'Queue[SessionPairResultsDto]', worker_id: int):
    log.info('Results worker starting.', results_worker=worker_id)
    # print(f'[INFO] ResultsWorker-{worker_id}: Results worker starting.')
    # await asyncio.sleep(10)

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
            pass # log.exception('Unknown exception in ResultsWorker.', results_worker=worker_id, exception=ex)
            # print(f'[ERROR] ResultsWorker-{worker_id}: Unknown exception in ResultsWorker.\n{ex}')


def results_worker_wrapper(results_queue: 'Queue[SessionPairResultsDto]', worker_id: int):
    asyncio.run(results_worker(results_queue, worker_id))


async def start_workers(loop):
    queue = Queue()
    results_queue = multiprocessing.Queue()

    workers = [http_worker_wrapper(queue, results_queue, x) for x in range(1)]
    workers = [queue_worker(queue)] + workers

    for x in range(3):
        Process(target=results_worker_wrapper, args=(results_queue, x)).start()

    await asyncio.gather(*workers)


async def main(loop):
    await start_workers(loop)


async def testmain(loop):
    pass


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
