from dataclasses import dataclass
from typing import Union, Dict

from aiohttp import ClientSession, ClientResponse
from multidict import CIMultiDictProxy


class HttpClientResponseDto:
    status: Union[int, None] = None
    reason: Union[str, None] = None
    content_type: Union[str, None] = None
    charset: Union[str, None] = None
    headers: Union[Dict[str, str], None] = None
    redirected: bool = False

    def __init__(self, client_response: Union[ClientResponse, None] = None):
        if client_response:
            self.status = client_response.status
            self.reason = client_response.reason
            self.content_type = client_response.content_type
            self.charset = client_response.charset
            self.redirected = len(client_response.history) != 0

            if client_response.headers:
                self._parse_headers(client_response.headers)

    def _parse_headers(self, headers: CIMultiDictProxy):
        self.headers = dict()
        for k, v in headers.items():
            self.headers[k] = v

    def __str__(self):
        return f'status="{self.status}", reason="{self.reason}", content_type="{self.content_type}" charset="{self.charset}" redirected="{self.redirected}"'


class SessionPairResultsDto:
    url: Union[str, None] = None
    client_response: Union[HttpClientResponseDto, None] = None
    response_body: Union[str, None] = None

    def __init__(self, session_pair: 'Union[SessionPair, None]', client_response: Union[HttpClientResponseDto, None], response_body: Union[str, None]):
        if session_pair:
            self.url = session_pair.url

        if client_response:
            self.client_response = client_response

        if response_body:
            self.response_body = response_body

    def __str__(self):
        return f'url="{self.url}", client_response="{self.client_response}" response_body="<{len(self.response_body) if self.response_body is not None else "None"}/REDACTED>"'


@dataclass
class SessionPair:
    session: ClientSession
    url: str

    def __str__(self):
        return f'url="{self.url}", session="<REDACTED>"'


@dataclass
class QueueObject:
    id: int
    url: str
    scheduled: str
    netloc: str


@dataclass
class VisitObject:
    id: int
    netloc: str
    time_stamp: str
