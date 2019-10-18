from typing import Union, Tuple

import aiofiles
import hashlib

from structlog import get_logger

from core.url_parse import parse_url, CCUrl
import os.path


STORAGE_DIRECTORY = '/media/puse/disk12/CoolCarabine/'


log = get_logger()


def hash_value(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


async def write_file(path: str, session_pair_results: SessionPairResults):
    try:
        async with aiofiles.open(path, 'w') as fd:
            await fd.write(session_pair_results.response_body)
    except Exception as ex:
        pass # log.exception('Unknown exception when writing to file.', path=path, url=session_pair_results.url, exception=ex)
        raise ex

async def store_index(session_pair_results: SessionPairResults, folder_path: str, url: str, name='index.html'):
    await write_file(os.path.join(folder_path, hash_value(name)), session_pair_results.response_body, url)


def get_path(session_pair_results: SessionPairResults) -> Tuple[str, str]:
    parsed: CCUrl = parse_url(session_pair_results.session_pair.url)
    netloc_hash = hash_value(parsed.urlparse.netloc)
    netloc_path = netloc_hash

    if parsed.urlparse.path == '' or parsed.urlparse.path == '/':
        return netloc_path, hash_value('index.html')

    page_path = parsed.urlparse.path.split('/')
    if page_path[0] == '':
        page_path = page_path[1:]

    if len(page_path) == 1:
        return netloc_path, hash_value(page_path[0])

    page_name = hash_value('index.html')
    if page_path[-1] != '':
        page_name = hash_value(page_path[-1])

    page_path = page_path[:-1]
    current_path = netloc_path
    for folder in page_path:
        current_path = os.path.join(current_path, hash_value(folder))

    path = os.path.join(current_path, page_name)

    return current_path, page_name


def path_contains_file(path: str) -> Tuple[bool, str]:
    splitted = os.path.split(path)
    current_path = ''

    for directory in splitted:
        current_path = os.path.join(current_path, directory)

        if not os.path.exists(os.path.join(STORAGE_DIRECTORY, current_path)):
            return False, current_path

        if os.path.isfile(os.path.join(STORAGE_DIRECTORY, current_path)):
            return True, current_path

    return False, current_path


def make_file_index(path: str, file_name: str):
    os.rename(
        os.path.join(STORAGE_DIRECTORY, path, file_name),
        os.path.join(STORAGE_DIRECTORY, path, f'tmp_{file_name}')
    )

    os.mkdir(os.path.join(STORAGE_DIRECTORY, path, file_name))

    os.rename(
        os.path.join(STORAGE_DIRECTORY, path, f'tmp_{file_name}'),
        os.path.join(STORAGE_DIRECTORY, path, file_name, hash_value('index.html'))
    )


def create_path_tree(path: str):
    splitted = os.path.split(path)
    current_path = ''

    for directory in splitted:
        old_path = current_path
        current_path = os.path.join(current_path, directory)
        full_path = os.path.join(STORAGE_DIRECTORY, current_path)

        if not os.path.exists(full_path):
            os.mkdir(full_path)
        else:
            if os.path.isfile(path):
                make_file_index(old_path, directory)


async def store_page(session_pair_results: SessionPairResults):
    path, file_name = get_path(session_pair_results)

    try:
        create_path_tree(path)
    except Exception as ex:
        # TODO logging
        pass # log.exception('Unknown exception when creating directory.', path=path, url=session_pair_results.url, exception=ex)
        return

    file_path = os.path.join(STORAGE_DIRECTORY, path, file_name)
    try:
        await write_file(file_path, session_pair_results)
    except Exception as ex:
        pass # log.exception('Unknown exception when writing to file', exception=ex, url=session_pair_results.url, file_path=file_path)
        return



