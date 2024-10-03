from __future__ import annotations

import asyncio
from datetime import datetime
from logging import getLogger
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union
from unittest.mock import Mock

from aiohttp import ClientResponse, ClientSession
from aiohttp.client_proto import ResponseHandler
from aiohttp.client_reqrep import RequestInfo
from aiohttp.helpers import BaseTimerContext
from aiohttp.streams import StreamReader
from aiohttp.tracing import Trace
from aiohttp.typedefs import RawHeaders
from multidict import CIMultiDict, CIMultiDictProxy, MultiDict, MultiDictProxy
from yarl import URL

from aiohttp_client_cache.cache_control import utcnow

JsonResponse = Optional[Dict[str, Any]]
DictItems = List[Tuple[str, str]]
LinkItems = List[Tuple[str, DictItems]]
LinkMultiDict = MultiDictProxy[MultiDictProxy[Union[str, URL]]]

logger = getLogger(__name__)


class CachedResponse(ClientResponse):
    """A dataclass containing cached response information, used for serialization.
    It will mostly behave the same as a :py:class:`aiohttp.ClientResponse` that has been read,
    with some additional cache-related info.
    """

    def __init__(
        self,
        method: str,
        url: URL,
        *,
        writer: asyncio.Task[None],
        continue100: asyncio.Future[bool] | None,
        timer: BaseTimerContext,
        request_info: RequestInfo,
        traces: list[Trace],
        loop: asyncio.AbstractEventLoop,
        session: ClientSession,
        # Attributes that `aiohttp` assigns when it calls `start()` under the hood.
        headers: CIMultiDictProxy,
        raw_headers: RawHeaders,
        status: int,
        history: tuple[ClientResponse, ...],
        body: Any,
        content: StreamReader,
        closed,
        protocol,
        connection,
        version,
        reason,
        cookies,
        released: bool,
    ) -> None:
        super().__init__(
            method,
            url,
            writer=writer,
            continue100=continue100,
            timer=timer,
            request_info=request_info,
            traces=traces,
            loop=loop,
            session=session,
        )
        self._content: StreamReader | None = None
        self.created_at: datetime = utcnow()
        self.expires: datetime | None = None
        self.last_used: datetime = utcnow()
        self.from_cache = True
        self._headers = headers
        self._raw_headers = raw_headers
        self.status = status
        self._history = history
        self._body = body
        self.content = content
        self._closed = closed
        self._protocol = protocol
        self._connection = connection
        self.version = version
        self.reason = reason
        self.cookies = cookies
        self._released = released

    def __getstate__(self):
        state = self.__dict__.copy()
        for k in (
            '_request_info',
            '_headers',
            '_cache',
            '_loop',
            '_timer',
            '_resolve_charset',
            '_protocol',
            '_content',
            '_session',
        ):
            del state[k]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._cache = {}
        self._content = None

        def decode_header(header):
            """Decode an individual (key, value) pair"""
            return (
                header[0].decode('utf-8', 'surrogateescape'),
                header[1].decode('utf-8', 'surrogateescape'),
            )

        self.headers = CIMultiDictProxy(CIMultiDict([decode_header(h) for h in self.raw_headers]))

    def get_encoding(self):
        return self._encoding

    @property  # type: ignore[override]
    def request_info(self) -> RequestInfo:
        return RequestInfo(
            url=self.url,
            method=self.method,
            headers=self.headers,
            real_url=self.url,
        )

    # NOTE: We redefine the same just to get rid of the `@reify' that protects against writing.
    @property  # type: ignore[override]
    def headers(self) -> CIMultiDictProxy[str]:
        return self._headers

    @headers.setter
    def headers(self, v) -> None:
        self._headers = v

    async def postprocess(self, expires: datetime | None = None) -> CachedResponse:
        """Read response content, and reset StreamReader on original response.

        This can be called only on an instance after `ClientSession._request()` returns `CachedResponse`
        because inside the `ClientSession._request()` headers are assigined at the very end (after `Response.start()`).
        """
        assert isinstance(expires, datetime) or expires is None, type(expires)

        if not self._released:
            await self.read()

        self.content = CachedStreamReader(self._body)

        self.expires = expires

        if self._history:
            self._history = (
                *[
                    await CachedResponse(
                        r.method,
                        r.url,
                        writer=r._writer,
                        continue100=r._continue,
                        timer=r._timer,
                        request_info=r._request_info,
                        traces=r._traces,
                        loop=r._loop,
                        session=r._session,
                        # Attributes that `aiohttp` assigns when it calls `start()` under the hood.
                        closed=r._closed,
                        protocol=r._protocol,
                        connection=r._connection,
                        version=r.version,
                        status=r.status,
                        reason=r.reason,
                        headers=r._headers,
                        raw_headers=r._raw_headers,
                        content=r.content,
                        cookies=r.cookies,
                        history=r._history,
                        body=r._body,
                        released=r._released,
                    ).postprocess()
                    for r in self._history
                ],
            )

        # We must call `get_encoding` before pickling because pickling `_resolve_charset` raises
        # _pickle.PicklingError: Can't pickle <function ClientSession.<lambda> at 0x7f94fdd13c40>:
        # attribute lookup ClientSession.<lambda> on aiohttp.client failed
        self._encoding: str = super().get_encoding()

        return self

    @property
    def content(self) -> StreamReader:
        if self._content is None:
            self._content = CachedStreamReader(self._body)
        return self._content

    @content.setter
    def content(self, value: StreamReader):
        self._content = value

    @property
    def is_expired(self) -> bool:
        """Determine if this cached response is expired"""
        try:
            return self.expires is not None and utcnow() > self.expires
        except (AttributeError, TypeError, ValueError):
            # Consider it expired and fetch a new response
            return True

    def reset(self):
        """Reset the stream reader to re-read a streamed response"""
        self._content = None


class CachedStreamReader(StreamReader):
    """A StreamReader loaded from previously consumed response content. This feeds cached data into
    the stream so it can support all the same behavior as the original stream: async iteration,
    chunked reads, etc.
    """

    def __init__(self, body: bytes | None = None):
        body = body or b''
        protocol = Mock(_reading_paused=False)
        super().__init__(protocol, limit=len(body), loop=None)
        self.feed_data(body)
        self.feed_eof()


AnyResponse = Union[ClientResponse, CachedResponse]


def _to_str_tuples(data: Mapping) -> DictItems:
    return [(k, str(v)) for k, v in data.items()]


def _to_url_multidict(data: DictItems) -> MultiDict:
    return MultiDict([(k, URL(url)) for k, url in data])
