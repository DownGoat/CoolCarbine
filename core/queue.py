import asyncio
import datetime
import random
from queue import Queue
from typing import Union, List

from asyncpg import Record
from structlog import get_logger

from core.database import get_connection
from core.url_parse import CCUrl
from domain import QueueObject

MAX_HOURLY_VISITS = 30
log = get_logger()


async def get_next_queue_items() -> List[str]:
    connection = await get_connection()
    values: List[Record] = await connection.fetch(
        '''select * from queue where scheduled < CURRENT_TIMESTAMP order by scheduled desc limit 50''')

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


async def get_latest_queued(netloc: str) -> Union[datetime.datetime, None]:
    connection = await get_connection()

    try:
        value = await connection.fetchrow(
            '''select * from queue where netloc = $1 order by scheduled desc limit 1''',
            netloc
        )

        if value:
            return value.get('scheduled')

        return None
    finally:
        await connection.close()


async def calculate_queue_time(netloc: str) -> datetime.datetime:
    last_queued = await get_latest_queued(netloc)
    if last_queued is None:
        return datetime.datetime.now()

    connection = await get_connection()

    try:
        hour_before = last_queued - datetime.timedelta(hours=1)
        count = (await connection.fetchrow(
            '''select count(id) from public.queue where netloc = $1 and scheduled > $2 and scheduled < $3;''',
            netloc,
            hour_before,
            last_queued
        )).get('count')

        timedelta = datetime.timedelta(minutes=random.randint(10, 50))
        scheduled_date = last_queued + timedelta

        if count < MAX_HOURLY_VISITS:
            scheduled_date = last_queued - timedelta

        return scheduled_date
    finally:
        await connection.close()


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


async def queue_url(connection, url: CCUrl):
    scheduled_time = await calculate_queue_time(url.urlparse.netloc)
    await connection.execute(
        '''insert into queue (url, netloc, scheduled) values ($1, $2, $3) on conflict do nothing;''',
        url.url, url.urlparse.netloc, scheduled_time
    )


async def add_to_queue(urls: List[CCUrl], worker_id: int):
    connection = await get_connection()
    tr = connection.transaction()

    try:
        await tr.start()
        for url in urls:
            await queue_url(connection, url)
    except Exception as ex:
        pass # log.exception('Unknown error when adding URLs to queue.', results_worker=worker_id, exception=ex)
        raise ex
    else:
        await tr.commit()
    finally:
        await connection.close()


async def queue_sleep(queue: 'Queue[str]'):
    continue_sleep = True

    while continue_sleep:
        if queue.qsize() < 50:
            continue_sleep = False
        else:
            log.info('Queue is full.', size=queue.qsize())
            await asyncio.sleep(0.5)


async def queue_worker(queue: 'Queue[str]'):
    log.info('Queue starting')
    while True:
        try:
            next_items = await get_next_queue_items()
            for item in next_items:
                queue.put(item)

            await queue_sleep(queue)
        except Exception as ex:
            pass # log.exception('Unknown exception in queue.', exception=ex)
        finally:
            await asyncio.sleep(1)
