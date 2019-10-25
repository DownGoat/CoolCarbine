import re
from typing import List
from urllib.parse import urlparse, urljoin, ParseResult

import validators
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError
from structlog import get_logger

log = get_logger()


class CCUrl:
    def __init__(self, url: str):
        self.url = url
        self.urlparse: ParseResult = urlparse(url)

    def is_relative(self) -> bool:
        if self.url.startswith('.'):
            return True
        if self.url.startswith('/') and not self.is_protocol_relative():
            return True

    def set_scheme(self, scheme: str):
        self.urlparse = self.urlparse._replace(scheme=scheme)
        self.url = self.urlparse.geturl()

    def is_protocol_relative(self) -> bool:
        return self.url.startswith('//')

    def _url_validator_regex_001(self) -> int:
        regex = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        return 1 if re.match(regex, self.url) else 0

    def _url_validator_django(self) -> int:
        validate = URLValidator()
        try:
            validate(self.url)
            return 1
        except ValidationError:
            return 0

    def _url_validator_validators(self):
        return 1 if validators.url(self.url) else 0

    def is_valid(self) -> bool:
        my_validators = [
            self._url_validator_regex_001(),
            self._url_validator_django(),
            self._url_validator_validators()
        ]

        valid = sum(my_validators) >= 2
        # if valid and not self.urlparse.netloc.lower().endswith('.no'):
        #    return False
        return valid

    def __str__(self):
        return self.url


def parse_url(url: str) -> CCUrl:
    return CCUrl(url)


def join_url(base: CCUrl, url: CCUrl, allow_fragments=True) -> CCUrl:
    return CCUrl(urljoin(base.url, url.url, allow_fragments))


def parse_extracted_url(base: CCUrl, href: str) -> CCUrl:
    href = href.strip()
    href_parsed = parse_url(href)

    if href_parsed.is_relative():
        return join_url(base, href_parsed)

    if href_parsed.is_protocol_relative():
        href_parsed.set_scheme(base.urlparse.scheme)

    return href_parsed


def filter_url(url: str) -> bool:
    # Filter out "empty" values.
    if url is None or url.strip() == '':
        return False

    lowercase_url = url.lower()

    # No fragments, mailto or tel
    if lowercase_url.startswith('#') or lowercase_url.startswith('mailto:') or lowercase_url.startswith('tel:'):
        return False

    return True


def filter_parsed_urls(parsed_urls: List[CCUrl], worker_id: int):
    filtered_urls: List[CCUrl] = []

    for url in parsed_urls:
        if url.is_valid() and url.urlparse.netloc.lower().endswith('.no'):
            filtered_urls.append(url)
        else:
            log.debug('Second pass filter removed a URL.', results_worker=worker_id, url=url.url)
    return filtered_urls


def parse_extracted_url_list(base: CCUrl, hrefs: List[str], worker_id: int) -> List[CCUrl]:
    filtered_hrefs: List[str] = []
    for href in hrefs:
        if filter_url(href):
            filtered_hrefs.append(href)

    parsed = [parse_extracted_url(base, href) for href in filtered_hrefs]

    return filter_parsed_urls(parsed, worker_id)
