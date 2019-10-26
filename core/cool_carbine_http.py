import asyncio
import socket
import ssl
import time
from queue import Queue, Empty

import aiohttp
from aiohttp import AsyncResolver
from structlog import get_logger

from domain import SessionPair, HttpClientResponseDto, SessionPairResultsDto
from config import HTTP_CONFIG

log = get_logger()


class AioHTTPWorker:
    def __init__(self, queue: 'Queue[str]', results_queue: 'Queue[SessionPairResultsDto]', config, worker_id: int):
        self._queue = queue
        self._results_queue = results_queue
        self._worker_id = worker_id
        self._config = config
        self._set_config()

    def _set_config(self):
        self._timeout = self._config.get('timeout', 15)
        self._headers = self._config.get('headers', {
            'User-Agent': 'Mozilla/5.0 (compatible; CoolCarbine/0.1-dev; +http://www.puse.cat/bot.html)'
        })
        self._resolver = AsyncResolver(nameservers=self._config.get('nameservers', ['1.1.1.1', '8.8.8.8']))

    def create_session(self):
        return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._timeout), headers=self._headers, connector=aiohttp.TCPConnector(resolver=self._resolver, family=socket.AF_INET, ssl=False))

    def get_log_info(self):
        return {'http_worker_name': self.__class__.__name__, 'http_worker_id': self._worker_id}

    async def fetch_url(self, session_pair: SessionPair) -> SessionPairResultsDto:
        log.info('Fetching URL.', url=session_pair.url, **self.get_log_info())
        t1_start = time.perf_counter()
        try:
            async with session_pair.session.get(session_pair.url) as response:
                log.info('Finished fetching URL.', url=session_pair.url, **self.get_log_info())
                return SessionPairResultsDto(session_pair, HttpClientResponseDto(response), await response.text())
        except ssl.SSLError as ex:
            log.exception('Unknown SSL error when fetching url.', exception=str(type(ex)), exception_message=str(ex), url=session_pair.url, **self.get_log_info())
        except TimeoutError:
            log.info('Request timed out.', url=session_pair.url, **self.get_log_info())
        except UnicodeDecodeError:
            log.info('Unicode decode error.', url=session_pair.url, **self.get_log_info())
        except Exception as ex:
            log.exception('Unknown exception when fetching url', exception=str(type(ex)), exception_message=str(ex), url=session_pair.url, **self.get_log_info())
        finally:
            t1_end = time.perf_counter()
            log.debug('Fetching URL perf_counter.', start=t1_start, end=t1_end, elapsed=t1_end - t1_start, url=session_pair.url, **self.get_log_info())
            await session_pair.session.close()

        return SessionPairResultsDto(session_pair, None, None)

    def get_session_pair(self, url: str) -> SessionPair:
        return SessionPair(self.create_session(), url)

    async def http_worker(self, url: str) -> SessionPairResultsDto:
        session_pair = self.get_session_pair(url)
        return await self.fetch_url(session_pair)

    async def start(self):
        while True:
            try:
                work = self._queue.get(timeout=1)
                result = await self.http_worker(work)
                self._results_queue.put(result)
                self._queue.task_done()
                await results_catch_up_waiter(self._results_queue, self._worker_id, type(AioHTTPWorker).__name__)
            except Empty:
                await asyncio.sleep(5)
            except Exception as ex:
                log.exception('Unknown exception in http handler', exception=str(type(ex)), exception_message=str(ex), **self.get_log_info())
                raise ex


async def results_catch_up_waiter(results_queue: 'Queue[SessionPairResultsDto]', worker_id: int, http_worker_name: str):
    while results_queue.qsize() > 100:
        log.info('Waiting for results queue to catch up.', queue_size=results_queue.qsize(), http_worker_id=worker_id, http_worker_name=http_worker_name)
        await asyncio.sleep(5)


async def start_aiohttp_module(queue: 'Queue[str]', results_queue: 'Queue[SessionPairResultsDto]', config, worker_id: int):
    worker = AioHTTPWorker(queue, results_queue, config, worker_id)
    await worker.start()


async def http_worker_wrapper(queue: 'Queue[str]', results_queue: 'Queue[SessionPairResultsDto]', worker_id: int):
    log.info('Starting HTTP worker.', http_worker=worker_id)
    await asyncio.sleep(10)
    http_module = HTTP_CONFIG.get('worker')

    if http_module is not None and http_module.get('name') == 'aiohttp':
        await start_aiohttp_module(queue, results_queue, http_module, worker_id)
    else:
        raise NotImplementedError('No other HTTP module is implemented at this time.')

