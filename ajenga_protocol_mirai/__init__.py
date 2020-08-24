import asyncio
import contextvars
import json
from dataclasses import dataclass
from dataclasses import field
from functools import wraps
from typing import List
from typing import Optional
from urllib.parse import urlparse
from urllib.request import url2pathname

import aiofiles
import aiohttp
import quart
from quart import Quart

import ajenga.message as raw_message
from ajenga.event import FriendMessageEvent
from ajenga.event import GroupMessageEvent
from ajenga.event import GroupMuteEvent
from ajenga.event import GroupPermission
from ajenga.event import GroupRecallEvent
from ajenga.event import GroupUnmuteEvent
from ajenga.event import Sender
from ajenga.event import TempMessageEvent
from ajenga.log import logger
from ajenga.message import ImageIdType
from ajenga.message import MessageChain
from ajenga.message import MessageElement
from ajenga.message import MessageIdType
from ajenga.message import Message_T
from ajenga.message import VoiceIdType
from ajenga.models import Group
from ajenga.models import GroupMember
from ajenga.protocol import Api
from ajenga.protocol import ApiResult
from ajenga.protocol import Code
from ajenga.protocol import MessageSendResult
from ajenga_app import BotSession
from ajenga_app import app
from .api import ApiError

logger = logger.getChild('mirai-protocol')

from . import api

upload_method = contextvars.ContextVar('upload_method')
METHOD_GROUP = 'group'
METHOD_FRIEND = 'friend'
METHOD_TEMP = 'temp'


@dataclass
class Source(raw_message.Meta):
    id: MessageIdType

    def raw(self) -> Optional[MessageElement]:
        return None


@dataclass
class Image(raw_message.Image):
    id: ImageIdType = None
    method: str = field(default_factory=upload_method.get)

    def __post_init__(self):
        super().__post_init__()
        if self.id:
            ind_start = self.id.index('{') + 1
            ind_end = self.id.index('}')
            self.hash = self.id[ind_start:ind_end].replace('-', '').lower()
        if not self.id and self.method:
            self.task = asyncio.create_task(self._prepare())
        else:
            self.task = None

    async def _prepare(self):
        if not self.content:
            logger.debug(f'Fetching image from url = {self.url}')
            url = urlparse(self.url)
            if url.scheme == 'file':
                async with aiofiles.open(url2pathname(url.path), 'rb') as f:
                    self.content = await f.read()
            else:
                async with aiohttp.request("GET", self.url) as resp:
                    self.content = await resp.content.read()
        bot = app.get_session(self.referer)
        img = await bot._upload_image(self.method, self.content, f'{self.hash}.png')
        logger.debug(f'Got resp !  {img}')
        self.id = img.id

    async def prepare(self, method=None):
        if self.id:
            return
        elif method and method != self.method:
            if self.task:
                logger.warning(f"Unused image upload cache {self.method} {method}")
            self.method = method
            return await self._prepare()
        elif self.task:
            await self.task
        else:
            raise ValueError("No upload method specified for Image!")

    def raw(self) -> Optional[MessageElement]:
        return raw_message.Image(url=self.url, content=self.content, hash=self.hash)

    def __eq__(self, other):
        return (isinstance(other, Image) and self.id == other.id) or super(Image, self).__eq__(other)


@dataclass
class Voice(raw_message.Voice):
    id: VoiceIdType = None
    method: str = field(default_factory=upload_method.get)

    def __post_init__(self):
        if self.id:
            self.hash = self.id[:self.id.find('.')].lower()


@dataclass
class Quote(raw_message.Quote):
    def raw(self) -> Optional[MessageElement]:
        return raw_message.Quote(id=self.id)


def _catch(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        while True:
            try:
                return await func(self, *args, **kwargs)
            except ApiError as e:
                if e.code == ApiError.CODE_SESSION_FAILED:
                    logger.info('Re-login Mirai')
                    new_session_key = await auth(api_root=self._api_root,
                                                 auth_key=self._auth_key)
                    if not new_session_key:
                        return ApiResult(Code.Unavailable)

                    self._api.update_session(new_session_key)

                    if await self._api.verify(qq=self.qq):
                        logger.info('Re-login Success')
                        continue
                    else:
                        return ApiResult(Code.Unavailable)
                else:
                    return ApiResult(e.code)
            except Exception as e:
                logger.exception(e)
                return ApiResult(Code.Unspecified, message=str(e))

    return wrapper


class MiraiSession(BotSession, Api):
    def __init__(self, api_root, auth_key, session_key, qq):
        self._qq = qq
        self._api_root = api_root
        self._auth_key = auth_key
        self._api = api.HttpApi(api_root, session_key, 30)

    @property
    def qq(self) -> int:
        return self._qq

    # Implement abstract function for BotSession

    @property
    def api(self) -> Api:
        return self

    def wrap_message(self, message: MessageElement, method=None) -> MessageElement:
        if message.referer == self.qq:
            return message
        message = message.raw()
        if isinstance(message, raw_message.Image):
            message2 = Image(url=message.url, content=message.content, method=method)
            message2.referer = self.qq
            return message2
        else:
            message.referer = self.qq
            return message

    # Implement abstract function for Api

    @_catch
    async def send_group_message(self, group: int, message: Message_T) -> ApiResult[MessageSendResult]:
        token = upload_method.set(METHOD_GROUP)
        msg = await self.prepare_message(message)
        if quote := msg.get_first(Quote):
            msg.remove(quote)
            res: dict = await self._api.sendGroupMessage(group=group, messageChain=self.as_mirai_chain(msg),
                                                         quote=quote.id)
        else:
            res: dict = await self._api.sendGroupMessage(group=group, messageChain=self.as_mirai_chain(msg))
        upload_method.reset(token)
        return ApiResult(res.get('code'), MessageSendResult(res.get('messageId')))

    @_catch
    async def send_friend_message(self, qq: int, message: Message_T) -> ApiResult[MessageSendResult]:
        token = upload_method.set(METHOD_FRIEND)
        msg = await self.prepare_message(message)
        res: dict = await self._api.sendFriendMessage(qq=qq, messageChain=self.as_mirai_chain(msg))
        upload_method.reset(token)
        return ApiResult(res.get('code'), MessageSendResult(res.get('messageId')))

    @_catch
    async def send_temp_message(self, qq: int, group: int, message: Message_T) -> ApiResult[MessageSendResult]:
        token = upload_method.set(METHOD_TEMP)
        msg = await self.prepare_message(message)
        res: dict = await self._api.sendTempMessage(qq=qq, group=group, messageChain=self.as_mirai_chain(msg))
        upload_method.reset(token)
        return ApiResult(res.get('code'), MessageSendResult(res.get('messageId')))

    @_catch
    async def recall(self, message_id: raw_message.MessageIdType) -> ApiResult[None]:
        res: dict = await self._api.recall(target=message_id)
        return ApiResult(res.get('code'))

    @_catch
    async def get_group_list(self) -> ApiResult[List[Group]]:
        res = await self._api.groupList(request_method='get')
        groups = []
        for g in res:
            groups.append(Group(
                id=g.get('id'),
                name=g.get('name'),
                permission=self._role_to_permission[g.get('permission')],
            ))
        return ApiResult(Code.Success, groups)

    @_catch
    async def get_group_member_list(self, group: int) -> ApiResult[List[GroupMember]]:
        res = await self._api.memberList(target=group, request_method='get')
        members = []
        for member in res:
            members.append(GroupMember(
                id=member.get('id'),
                name=member.get('memberName'),
                permission=self._role_to_permission[member.get('permission')],
            ))
        return ApiResult(Code.Success, members)

    @_catch
    async def get_group_member_info(self, group: int, qq: int) -> ApiResult[GroupMember]:
        res = await self._api.memberInfo(target=group, memberId=qq, request_method='get')
        return ApiResult(Code.Success, GroupMember(
            id=res.get('id'),
            name=res.get('memberName'),
            permission=self._role_to_permission[res.get('permission')],
        ))

    # Message Utils

    async def _upload_image(self, method, img: bytes, name: str):
        # return await self.call_action_(action="uploadImage", type=type_, img=img)
        res = await self._api.upload_image(filedata=img, filename=name, type=method)
        img_msg = Image(url=res['url'], id=res['imageId'], method=method)
        img_msg.referer = self.qq
        return img_msg

    async def _upload_record(self, method, voice: bytes, name: str):
        # return await self.call_action_(action="uploadImage", type=type_, img=img)
        res = await self._api.upload_voice(filedata=voice, filename=name, type=method)
        voice_msg = Voice(id=res['voiceId'], method=method)
        voice_msg.referer = self.qq
        return voice_msg

    # Message Transition

    def as_message_el(self, msg: dict) -> MessageElement:
        # print(msg)
        type_: str = msg['type']
        if type_ == 'Source':
            ret = Source(id=msg['id'])
        elif type_ == 'Plain':
            ret = raw_message.Plain(text=msg['text'])
        elif type_ == 'Quote':
            ret = Quote(id=msg['id'], origin=self.as_message_chain(msg['origin']))
        elif type_ == 'At':
            ret = raw_message.At(msg['target'])
        elif type_ == 'AtAll':
            ret = raw_message.AtAll()
        elif type_ == 'Face':
            ret = raw_message.Face(msg['faceId'])
        elif type_ == 'Image':
            ret = Image(url=msg['url'], id=msg['imageId'])
        elif type_ == 'Voice':
            ret = Voice(url=msg['url'], id=msg['voiceId'])
        elif type_ == 'App':
            ret = raw_message.App(content=json.loads(msg['content']))
        elif type_ == 'Xml':
            ret = raw_message.Xml(content=msg['xml'])
        else:
            ret = raw_message.Unknown()
        ret.referer = self.qq
        return ret

    def as_mirai_el(self, msg: MessageElement) -> dict:
        if isinstance(msg, raw_message.Plain):
            return {
                'type': 'Plain',
                'text': msg.text,
            }
        elif isinstance(msg, raw_message.At):
            return {
                'type': 'At',
                'target': msg.target,
            }
        elif isinstance(msg, raw_message.AtAll):
            return {
                'type': 'AtAll',
            }
        elif isinstance(msg, Image):
            return {
                'type': 'Image',
                'imageId': msg.id,
            }
        elif isinstance(msg, raw_message.Face):
            return {
                'type': 'Face',
                'faceId': msg.id,
            }
        elif isinstance(msg, Voice):
            return {
                'type': 'Voice',
                'voiceId': msg.id,
            }
        elif isinstance(msg, raw_message.Voice):
            return {
                'type': 'Voice',
                'url': msg.url,
            }
        elif isinstance(msg, raw_message.App):
            return {
                'type': 'App',
                'content': json.dumps(msg.content),
            }
        elif isinstance(msg, raw_message.Xml):
            return {
                'type': 'Xml',
                'xml': msg.content,
            }
        else:
            # print('???? ', msg)
            return {}

    def as_message_chain(self, chain: list) -> MessageChain:
        return MessageChain(filter(None, [self.as_message_el(x) for x in chain]))

    def as_mirai_chain(self, chain: MessageChain) -> list:
        return list(filter(None, [self.as_mirai_el(x) for x in chain]))

    _role_to_permission = {
        'OWNER': GroupPermission.OWNER,
        'ADMINISTRATOR': GroupPermission.ADMIN,
        'MEMBER': GroupPermission.MEMBER,
    }

    def as_event(self, mi_event):
        type_ = mi_event['type']
        if type_ == 'GroupMessage':
            token = upload_method.set(METHOD_GROUP)

            msg = self.as_message_chain(mi_event['messageChain'])
            event = GroupMessageEvent(
                message=msg,
                message_id=msg[0].id,
                group=mi_event['sender']['group']['id'],
                sender=Sender(
                    qq=mi_event['sender']['id'],
                    name=mi_event['sender']['memberName'],
                    permission=self._role_to_permission[mi_event['sender']['permission']],
                ),
            )

            upload_method.reset(token)
            return event
        elif type_ == 'FriendMessage':
            token = upload_method.set(METHOD_FRIEND)

            msg = self.as_message_chain(mi_event['messageChain'])
            event = FriendMessageEvent(
                message=msg,
                message_id=msg[0].id,
                sender=Sender(
                    qq=mi_event['sender']['id'],
                    name=mi_event['sender']['nickname'],
                ),
            )

            upload_method.reset(token)
            return event
        elif type_ == 'TempMessage':
            token = upload_method.set(METHOD_TEMP)

            msg = self.as_message_chain(mi_event['messageChain'])
            event = TempMessageEvent(
                message=msg,
                message_id=msg[0].id,
                sender=Sender(
                    qq=mi_event['sender']['id'],
                    name=mi_event['sender']['memberName'],
                ),
                group=mi_event['sender']['group']['id'],
            )

            upload_method.reset(token)
            return event
        elif type_ == 'GroupRecallEvent':
            event = GroupRecallEvent(
                qq=mi_event['authorId'],
                message_id=mi_event['messageId'],
                group=mi_event['group']['id'],
                operator=mi_event['operator']['id'] if mi_event['operator'] else self.qq,
            )
            return event
        elif type_ == 'BotMuteEvent':
            event = GroupMuteEvent(
                qq=self.qq,
                operator=mi_event['operator']['id'] if mi_event['operator'] else self.qq,
                duration=mi_event['durationSeconds'],
            )
            return event
        elif type_ == 'MemberMuteEvent':
            event = GroupMuteEvent(
                qq=mi_event['member']['id'],
                operator=mi_event['operator']['id'] if mi_event['operator'] else self.qq,
                duration=mi_event['durationSeconds'],
            )
            return event
        elif type_ == 'BotUnmuteEvent':
            event = GroupUnmuteEvent(
                qq=self.qq,
                operator=mi_event['operator']['id'] if mi_event['operator'] else self.qq,
            )
            return event
        elif type_ == 'MemberUnmuteEvent':
            event = GroupUnmuteEvent(
                qq=mi_event['member']['id'],
                operator=mi_event['operator']['id'] if mi_event['operator'] else self.qq,
            )
            return event
        return None

    async def prepare_message(self, message: Message_T) -> MessageChain:
        # print('send: ', raw_message)
        if isinstance(message, MessageChain):
            message = message.to(self, method=upload_method.get())
        else:
            message = MessageChain(message).to(self, method=upload_method.get())
        # print('send now: ', raw_message)
        for msg in message:
            if isinstance(msg, Image):
                await msg.prepare(method=upload_method.get())
        # logger.debug(f'send now:  {raw_message}')
        return message

    def set_report(self, report_path: str, **kwargs):
        self._server_app = Quart('')

        @self._server_app.route(report_path, methods=['POST'])
        async def _on_report():
            event = await quart.request.get_json()
            logger.debug(event)

            if event := self.as_event(event):
                self.handle_event_nowait(event)

            return {'code': 0}

        return asyncio.create_task(self._server_app.run_task(use_reloader=False, **kwargs))


async def auth(api_root, auth_key):
    async with aiohttp.request("POST", api_root + 'auth', json={'authKey': auth_key}) as resp:
        if 200 <= resp.status < 300:
            result = json.loads(await resp.text())
            logger.info(f'Login Mirai: {result}')
            if result.get('code') == 0:
                return result['session']


async def connect(host, port, auth_key, qq) -> Optional[MiraiSession]:
    api_root = f'http://{host}:{port}/'

    session_key = await auth(api_root=api_root,
                             auth_key=auth_key)

    if not session_key:
        return

    session = MiraiSession(api_root=api_root,
                           auth_key=auth_key,
                           session_key=session_key,
                           qq=qq)
    if await session._api.verify(qq=qq):
        return session
