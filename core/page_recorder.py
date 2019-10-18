from typing import List, Union
from urllib.parse import urlparse, ParseResult

from structlog import get_logger

from core.database import get_connection
from core.url_parse import CCUrl
from domain import SessionPairResultsDto


log = get_logger()


async def get_page_record(url: str, netloc: str, worker_id: int) -> Union[int, None]:
    connection = await get_connection()

    try:
        found = await connection.fetchrow(
            '''select id from page where netloc = $1 and url = $2;''',
            netloc,
            url
        )

        return found.get('id', None) if found else None
    except Exception as ex:
        pass # log.exception('Unknown exception when fetching page record.', results_worker=worker_id, exception=ex)
    finally:
        await connection.close()

    return None


async def create_page_record(url: str, netloc: str, worker_id: int, connection=None) -> Union[int, None]:
    transaction = connection is not None

    if not transaction:
        connection = await get_connection()

    try:
        page_id = (await connection.fetchrow(
            '''insert into page (netloc, url) values ($1, $2) on conflict do nothing returning id;''',
            netloc,
            url
        )).get('id')

        return page_id
    except Exception as ex:
        pass # log.exception('Unknown exception when creating page record.', results_worker=worker_id, exception=ex)
    finally:
        if not transaction:
            await connection.close()

    return None


async def create_page_connection(connection, found_on_page: int, extracted_page_id: int, worker_id: int):
    try:
        await connection.execute(
            '''insert into page_x_page (found_on_page_id, page_id) values ($1, $2);''',
            found_on_page,
            extracted_page_id
        )
    except Exception as ex:
        pass # log.exception('Unknown exception when creating page x page record.', results_worker=worker_id, exception=ex)


async def record_page_connections(extracted_urls: List[CCUrl], session_pair_results: SessionPairResultsDto, worker_id: int):
    parsed_url: ParseResult = urlparse(session_pair_results.url)
    page_id = await get_page_record(session_pair_results.url, parsed_url.netloc, worker_id)

    if not page_id:
        page_id = await create_page_record(session_pair_results.url, parsed_url.netloc, worker_id)

    connection = await get_connection()
    tr = connection.transaction()
    try:
        await tr.start()
        for url in extracted_urls:
            extracted_page_id = await create_page_record(url.url, url.urlparse.netloc, worker_id, connection)
            await create_page_connection(connection, page_id, extracted_page_id, worker_id)
    except Exception as ex:
        await tr.rollback()
        pass # log.exception('Unknown exception when creating page connection.', results_worker=worker_id, exception=ex)
    else:
        await tr.commit()
    finally:
        await connection.close()
