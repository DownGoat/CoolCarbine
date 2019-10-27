import asyncio
import datetime
import random
import time
from queue import Queue
from typing import Union, List, Dict, Tuple

from asyncpg import Record
from structlog import get_logger

import config
from core.database import get_connection
from core.url_parse import CCUrl
from domain import QueueObject

MAX_HOURLY_VISITS = config.MAX_HOURLY_VISITS
log = get_logger()


async def get_next_queue_items() -> List[str]:
    connection = await get_connection()
    values: List[Record] = await connection.fetch(
        '''select * from queue where scheduled < CURRENT_TIMESTAMP order by scheduled desc limit 300''')

    netlocs = dict()
    queue: List[QueueObject] = []
    for value in values:
        q = QueueObject(**dict(value))
        netlocs[q.netloc] = True
        queue.append(q)

    hour_ago = datetime.datetime.now() - datetime.timedelta(hours=1)
    visits: List[Record] = await connection.fetch(
        '''select netloc, count(netloc) from visits where time_stamp > $1 and netloc = any($2::varchar[]) group by netloc''',
        hour_ago, list(netlocs.keys()))

    visits_map = dict()
    for visit in visits:
        visits_map[visit.get('netloc')] = visit.get('count')

    # Filter out excessive request to a single netloc
    filtered_ids: List[int] = []
    filtered_queue: List[QueueObject] = []
    queue_map = dict()
    for item in queue:
        if item.netloc in queue_map:
            queue_map[item.netloc] += 1
        else:
            queue_map[item.netloc] = 1

        if visits_map.get(item.netloc, 0) + queue_map[item.netloc] < MAX_HOURLY_VISITS:
            filtered_queue.append(item)
        else:
            filtered_ids.append(item.id)
            log.info('Request to netloc blocked due to reaching max hourly visits.', limit=MAX_HOURLY_VISITS, netloc=item.netloc)

    hour_from_now = datetime.datetime.now() + datetime.timedelta(hours=1)
    await connection.execute('''UPDATE queue SET scheduled = $1 WHERE id = any($2::int[])''', hour_from_now, filtered_ids)

    ids = [item.id for item in filtered_queue]

    # delete the ones we are crawling.
    await connection.execute('''delete from queue where id = any($1::int[])''', ids)

    await connection.close()
    return [item.url for item in filtered_queue]


async def check_if_queued(url: str) -> bool:
    connection = await get_connection()

    try:
        value = await connection.fetchrow(
            '''select id from queue where url = $1;''',
            url
        )

        return value is not None
    finally:
        await connection.close()


async def queue_url(connection, url: CCUrl, scheduled_time: datetime.datetime):
    await connection.execute(
        '''insert into queue (url, netloc, scheduled) values ($1, $2, $3) on conflict do nothing;''',
        url.url, url.urlparse.netloc, scheduled_time
    )


async def get_netloc_schedules(urls: List[CCUrl], worker_id: int) -> Dict[str, datetime.datetime]:
    connection = await get_connection()

    try:
        netlocs = [url.urlparse.netloc for url in urls]
        log.debug('getting netloc schedule', length=len(netlocs), results_worker=worker_id)

        t1_start = time.perf_counter()
        values = await connection.fetch(
            '''select distinct on(netloc) netloc, scheduled from queue where netloc = any($1::varchar[]) order by netloc, scheduled desc;''',
            netlocs
        )
        t1_end = time.perf_counter()
        log.debug('netloc query time', elapsed=t1_end-t1_start, results_worker=worker_id)
        latest_schedule: Dict[str, datetime] = dict()
        for v in values:
            latest_schedule[v.get('netloc')] = v.get('scheduled')

        return latest_schedule
    except Exception as ex:
        log.exception('Unknown error when fetching schedule for netlocs.', results_worker=worker_id, exception=str(type(ex)), exception_message=str(ex))
        raise ex
    finally:
        await connection.close()


async def add_to_queue(urls: List[CCUrl], worker_id: int):
    connection = await get_connection()
    tr = connection.transaction()

    try:
        t1_start = time.perf_counter()
        netlocs_schedule = await get_netloc_schedules(urls, worker_id)
        url_schedule: List[Tuple[CCUrl, datetime.datetime]] = []
        for url in urls:
            latest_schedule = netlocs_schedule.get(url.urlparse.netloc, datetime.datetime.now() - datetime.timedelta(minutes=6))
            next_schedule = latest_schedule + datetime.timedelta(minutes=6)
            netlocs_schedule[url.urlparse.netloc] = next_schedule
            url_schedule.append((url, next_schedule))
        t1_end = time.perf_counter()

        log.debug('perf_counter add_to_queue netlocs', elapsed=t1_end - t1_start, results_worker=worker_id)

        t1_start = time.perf_counter()
        await tr.start()
        for url, scheduled_time in url_schedule:
            await queue_url(connection, url, scheduled_time)
        await tr.commit()
        t1_end = time.perf_counter()

        log.debug('perf_counter add_to_queue transaction', elapsed=t1_end - t1_start, results_worker=worker_id)

    except Exception as ex:
        log.exception('Unknown error when adding URLs to queue.', results_worker=worker_id, exception=str(type(ex)), exception_message=str(ex))
    finally:
        await connection.close()


async def queue_sleep(queue: 'Queue[str]'):
    continue_sleep = True
    log.info('Queue sleeping.')
    while continue_sleep:
        if queue.qsize() < 250:
            continue_sleep = False
        else:
            log.info('Queue is full.', size=queue.qsize())
            await asyncio.sleep(5)


async def queue_worker(queue: 'Queue[str]'):
    log.info('Queue starting')
    while True:
        try:
            next_items = await get_next_queue_items()
            for item in next_items:
                queue.put(item)

            await queue_sleep(queue)
        except Exception as ex:
            log.exception('Something went wrong when fetching queue items.')
            # pass # log.exception('Unknown exception in queue.', exception=ex)
            raise ex
        finally:
            await asyncio.sleep(1)
