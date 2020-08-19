import functools
import abc
from typing import Callable, Dict, Any, Optional, Union, Awaitable

import aiohttp

try:
    import ujson as json
except ImportError:
    import json

from . import logger


class ApiError(Exception):
    CODE_REQUEST_ERROR = -3
    CODE_NETWORK_ERROR = -2
    CODE_NOT_AVAILABLE = -1

    def __init__(self, code: int, message: str = 'fail'):
        self.code = code
        self.message = message

    def __repr__(self):
        return f'<ApiError, code={self.code}, message={self.message}>'

    def __str__(self):
        return self.__repr__()


class Api:

    @abc.abstractmethod
    def call_action(self, action: str, **params) -> Union[Awaitable[Any], Any]:

        pass

    def __getattr__(self,
                    item: str) -> Callable[..., Union[Awaitable[Any], Any]]:

        return functools.partial(self.call_action, item)


class AsyncApi(Api):

    @abc.abstractmethod
    async def call_action(self, action: str, **params) -> Any:
        pass


def _handle_api_result(result: Optional[Dict[str, Any]]) -> Any:
    return result


class HttpApi(AsyncApi):

    def __init__(self,
                 api_root: Optional[str],
                 session_key: Optional[str],
                 timeout_sec: float):
        super().__init__()
        self._api_root = api_root.rstrip('/') + '/' if api_root else None
        self._session_key = session_key
        self._timeout_sec = timeout_sec

    async def call_action(self, action: str, request_method='post', **params) -> Any:
        if not self._api_root:
            raise ApiError(code=ApiError.CODE_NOT_AVAILABLE, message="Api not available")

        headers = {}

        params['sessionKey'] = self._session_key
        logger.debug(f'call {action} {request_method} {self._api_root + action} {params}')

        try:

            if request_method == 'post':
                req_cm = aiohttp.request("POST", self._api_root + action,
                                         json=params, headers=headers)
            elif request_method == 'get':
                req_cm = aiohttp.request("GET", self._api_root + action,
                                         params=params, headers=headers)
            else:
                return

            async with req_cm as resp:
                if 200 <= resp.status < 300:
                    result = await resp.json()
                    logger.debug(f'recv resp: {result}')
                    return _handle_api_result(result)
                else:
                    raise ApiError(code=ApiError.CODE_NETWORK_ERROR,
                                   message=f'{resp.status}')
        except aiohttp.ClientError as e:
            raise ApiError(code=ApiError.CODE_REQUEST_ERROR,
                           message=str(e))

    async def upload(self, filedata: bytes, filename: str, **params):
        upload_data = aiohttp.FormData()
        upload_data.add_field("img", filedata, filename=filename)
        upload_data.add_field("sessionKey", self._session_key)
        for item in params.items():
            upload_data.add_fields(item)
        try:

            async with aiohttp.ClientSession() as session:
                async with session.post(self._api_root + "uploadImage", data=upload_data) as resp:
                    if 200 <= resp.status < 300:
                        result = await resp.json()
                        logger.debug(f'recv resp: {result}')
                        return result
                    else:
                        raise ApiError(code=ApiError.CODE_NETWORK_ERROR,
                                       message=f'{resp.status}')

        except aiohttp.ClientError as e:
            raise ApiError(code=ApiError.CODE_REQUEST_ERROR,
                           message=str(e))
