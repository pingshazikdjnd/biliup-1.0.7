"""
https://github.com/bugov/aiohttp-basicauth-middleware
"""

import hmac
import inspect
import logging
from typing import (
    Callable,
    Iterable,
    Type,
)
from aiohttp import web
from .http_basic_auth import parse_header, BasicAuthException


log = logging.getLogger(__name__)


def _safe_compare(expected, actual) -> bool:
    """使用恒定时间比对凭据，兼容非 ASCII 字符串并防御类型异常"""
    if expected is None or actual is None:
        return False
    if isinstance(expected, str):
        expected = expected.encode('utf-8')
    if isinstance(actual, str):
        actual = actual.encode('utf-8')
    if not isinstance(expected, (bytes, bytearray)) or not isinstance(actual, (bytes, bytearray)):
        return False
    return hmac.compare_digest(expected, actual)


class BaseStrategy:
    def __init__(self, request: web.Request, storage: dict, handler: Callable, header: str):
        self.request = request
        self.storage = storage
        self.handler = handler
        self.header = header

        log.debug('Init strategy %r', (self.request, self.storage, self.handler))

    def get_credentials(self) -> tuple[str, str]:
        try:
            return parse_header(self.header)
        except BasicAuthException:
            log.info('Invalid basic auth header: %r', self.header)
            self.on_error()

    async def password_test(self) -> bool:
        login, password = self.get_credentials()
        server_password = self.storage.get(login)
        return _safe_compare(server_password, password)

    async def check(self) -> web.Response:
        if await self.password_test():
            return await self.handler(self.request)

        self.on_error()

    def on_error(self):
        raise web.HTTPUnauthorized()


def check_access(
    auth_dict: dict,
    header_value: str,
    strategy: Callable = lambda x: x
) -> bool:
    try:
        login, password = parse_header(header_value)
    except BasicAuthException as e:
        log.debug('Invalid basic auth header: %s', e)
        return False

    hashed_password = auth_dict.get(login)
    if hashed_password is None:
        return False
    hashed_request_password = strategy(password)
    return _safe_compare(hashed_password, hashed_request_password)


def basic_auth_middleware(
    urls: Iterable,
    auth_dict: dict,
    strategy: Type[BaseStrategy] = lambda x: x,
    query_auth_prefixes: Iterable = ()
):
    protected_urls = tuple(urls)
    query_prefixes = tuple(query_auth_prefixes)

    @web.middleware
    async def middleware(request: web.Request, handler: Callable) -> web.Response:
        if request.method == 'OPTIONS':
            return await handler(request)

        for url in protected_urls:
            if not request.path.startswith(url):
                continue

            auth_header = request.headers.get('Authorization', '')
            if (
                not auth_header
                and request.method in ('GET', 'HEAD')
                and any(request.path.startswith(prefix) for prefix in query_prefixes)
            ):
                auth_header = request.query.get('authorization', '')

            if inspect.isclass(strategy) and issubclass(strategy, BaseStrategy):
                log.debug("Use Strategy: %r", strategy.__name__)
                strategy_obj = strategy(
                    request,
                    auth_dict,
                    handler,
                    auth_header
                )
                return await strategy_obj.check()

            if not check_access(auth_dict, auth_header, strategy):
                raise web.HTTPUnauthorized()

            return await handler(request)
        return await handler(request)

    return middleware
