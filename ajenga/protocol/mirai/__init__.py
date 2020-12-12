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

import ajenga.event as raw_event
import ajenga.message as raw_message
from ajenga.event import Event
from ajenga.event import FriendMessageEvent
from ajenga.event import GroupMessageEvent
from ajenga.event import GroupMuteEvent
from ajenga.event import GroupPermission
from ajenga.event import GroupRecallEvent
from ajenga.event import GroupUnmuteEvent
from ajenga.event import MessageEvent
from ajenga.event import Sender
from ajenga.event import TempMessageEvent
from ajenga.log import logger
from ajenga.message import ImageIdType
from ajenga.message import MessageChain
from ajenga.message import MessageElement
from ajenga.message import MessageIdType
from ajenga.message import Message_T
from ajenga.message import VoiceIdType
from ajenga.models import ContactIdType
from ajenga.models import Friend
from ajenga.models import Group
from ajenga.models import GroupMember
from ajenga.protocol import Api
from ajenga.protocol import ApiResult
from ajenga.protocol import Code
from ajenga.protocol import MessageSendResult
from ajenga.app import BotSession
from ajenga import app
from ajenga.ctx import this
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

    async def raw(self) -> Optional[MessageElement]:
        return None


@dataclass
class Image(raw_message.Image):
    id: ImageIdType = None
    method: str = field(default_factory=upload_method.get)

    def __post_init__(self):
        super().__post_init__()
        if self.id and self.method == METHOD_GROUP:
            ind_start = self.id.index('{') + 1
            ind_end = self.id.index('}')
            self.hash = self.id[ind_start:ind_end].replace('-', '').lower()
        elif self.id and self.method == METHOD_FRIEND:
            ind_start = self.id.rfind('-') + 1
            self.hash = self.id[ind_start:].lower()
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
                    self.set_content(await f.read())
            else:
                async with aiohttp.request("GET", self.url) as resp:
                    self.set_content(await resp.content.read())
        bot: Optional[MiraiSession] = app.get_session(self.referer)
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

    async def raw(self) -> Optional[MessageElement]:
        return raw_message.Image(url=self.url, content=self.content, hash=self.hash)

    def __eq__(self, other):
        return (isinstance(other, Image) and self.id == other.id) or super(Image, self).__eq__(other)


@dataclass
class Voice(raw_message.Voice):
    id: VoiceIdType = None
    # json: str = None
    method: str = field(default_factory=upload_method.get)

    def __post_init__(self):
        if self.id:
            self.hash = self.id[:self.id.find('.')].lower()
        if self.url:
            self.url = self.convert_url(self.url, 0, 1) or self.url

    @staticmethod
    def convert_url(url: str, old: int, new: int):
        if url.find(f'voice_codec={old}') == -1:
            return
        url = url.replace(f'voice_codec={old}', f'voice_codec={new}')
        lurl = list(url)
        lurl[url.index('&filetype') - 5] = hex(int(lurl[url.index('&filetype') - 5], base=16) + new - old)[2:]
        t = int(url[url.index('rkey=') + 133:url.index('rkey=') + 135], base=16) - new + old
        lurl[url.index('rkey=') + 133:url.index('rkey=') + 135] = f"{t:02x}"

        url = ''.join(lurl)
        return url


@dataclass
class Quote(raw_message.Quote):
    async def raw(self) -> Optional[MessageElement]:
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

                    if not await self.relogin():
                        return ApiResult(Code.Unavailable)

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


@dataclass
class FriendAddRequestEvent(raw_event.FriendAddRequestEvent):
    event_id: str

    @_catch
    async def _reply(self, operate: int, message: str = ''):
        session: MiraiSession = this.bot
        await session._api.call_action(action='resp/newFriendRequestEvent',
                                       eventId=self.event_id,
                                       fromId=self.qq,
                                       groupId=0,
                                       message=message,
                                       operate=operate,
                                       )

    async def accept(self, **kwargs):
        return await self._reply(0)

    async def reject(self, **kwargs):
        return await self._reply(1)

    async def ignore(self):
        return await self._reply(2)


@dataclass
class GroupJoinRequestEvent(raw_event.GroupJoinRequestEvent):
    event_id: str

    @_catch
    async def _reply(self, operate: int, message: str = ''):
        session: MiraiSession = this.bot
        await session._api.call_action(action='resp/memberJoinRequestEvent',
                                       eventId=self.event_id,
                                       fromId=self.qq,
                                       groupId=self.group,
                                       message=message,
                                       operate=operate,
                                       )

    async def accept(self, **kwargs):
        return await self._reply(0)

    async def reject(self, **kwargs):
        return await self._reply(1)

    async def ignore(self):
        return await self._reply(2)


@dataclass
class GroupInvitedRequestEvent(raw_event.GroupInvitedRequestEvent):
    event_id: str

    @_catch
    async def _reply(self, operate: int, message: str = ''):
        session: MiraiSession = this.bot
        await session._api.call_action(action='resp/botInvitedJoinGroupRequestEvent',
                                       eventId=self.event_id,
                                       fromId=self.operator,
                                       groupId=self.group,
                                       groupName="",
                                       message=message,
                                       operate=operate,
                                       )

    async def accept(self, **kwargs):
        return await self._reply(0)

    async def reject(self, **kwargs):
        return await self._reply(1)

    async def ignore(self):
        pass


class MiraiSession(BotSession, Api):

    def __init__(self, host, port, *, auth_key='', qq, enable_ws=False, enable_poll=False, report_path=None, max_retries=0, retry_interval=30):
        self._qq = qq
        self._host = host
        self._port = port
        self._api_root = f'http://{self._host}:{self._port}/'
        self._auth_key = auth_key
        self._max_retries = max_retries
        self._retry_interval = retry_interval
        self._enable_ws = enable_ws
        self._enable_poll = enable_poll
        self._report_path = report_path
        self._session_key = None
        self._api = None
        self._app = Quart('')
        self._app.before_serving(self.connect)
        if self._report_path:
            self.set_report(self._report_path)

        self._ok = False

    async def connect(self):
        retries = 0
        session_key = await self._auth()
        while not session_key and (retries < self._max_retries or self._max_retries < 0):
            retries += 1
            logger.info(f"Connection failed. Retrying {retries}th time in {self._retry_interval}s ...")
            await asyncio.sleep(self._retry_interval)
            session_key = await self._auth()

        if not session_key:
            logger.critical(f"Connection failed. Exiting ...")
            return

        self._session_key = session_key
        self._api = api.HttpApi(self._api_root, session_key, 30)

        if not await self._api.verify(qq=self._qq):
            raise api.ApiError(api.Code.Unavailable, "Verify Failed")

        self._ok = True

        if self._enable_ws:
            asyncio.create_task(self.set_report_ws())
        if self._enable_poll:
            asyncio.create_task(self.set_poll())

    async def _auth(self):
        try:
            async with aiohttp.request("POST", self._api_root + 'auth', json={'authKey': self._auth_key}) as resp:
                if 200 <= resp.status < 300:
                    result = json.loads(await resp.text())
                    logger.info(f'Login Mirai: {result}')
                    if result.get('code') == 0:
                        return result['session']
        except Exception as e:
            logger.error(e)
            return

    @property
    def qq(self) -> int:
        return self._qq

    # Implement abstract function for BotSession

    @property
    def ok(self) -> bool:
        return True

    @property
    def asgi(self):
        return self._app.asgi_app

    @property
    def api(self) -> Api:
        return self

    async def wrap_message(self, message: MessageElement, method=None) -> MessageElement:
        if message.referer == self.qq:
            return message
        message = await message.raw()
        if isinstance(message, raw_message.Image):
            message2 = Image(url=message.url, content=message.content, method=method)
            message2.referer = self.qq
            return message2
        else:
            message.referer = self.qq
            return message

    # Implement abstract function for Api

    @_catch
    async def send_group_message(self,
                                 group: ContactIdType,
                                 message: Message_T,
                                 ) -> ApiResult[MessageSendResult]:
        token = upload_method.set(METHOD_GROUP)
        msg = await self.prepare_message(message)
        quote = msg.get_first(Quote)
        if quote:
            msg.remove(quote)
            res: dict = await self._api.sendGroupMessage(group=group, messageChain=self.as_mirai_chain(msg),
                                                         quote=quote.id)
        else:
            res: dict = await self._api.sendGroupMessage(group=group, messageChain=self.as_mirai_chain(msg))
        upload_method.reset(token)
        return ApiResult(res.get('code'), MessageSendResult(res.get('messageId')))

    @_catch
    async def send_friend_message(self,
                                  qq: ContactIdType,
                                  message: Message_T,
                                  ) -> ApiResult[MessageSendResult]:
        token = upload_method.set(METHOD_FRIEND)
        msg = await self.prepare_message(message)
        res: dict = await self._api.sendFriendMessage(qq=qq, messageChain=self.as_mirai_chain(msg))
        upload_method.reset(token)
        return ApiResult(res.get('code'), MessageSendResult(res.get('messageId')))

    @_catch
    async def send_temp_message(self,
                                qq: ContactIdType,
                                group: ContactIdType,
                                message: Message_T,
                                ) -> ApiResult[MessageSendResult]:
        token = upload_method.set(METHOD_TEMP)
        msg = await self.prepare_message(message)
        res: dict = await self._api.sendTempMessage(qq=qq, group=group, messageChain=self.as_mirai_chain(msg))
        upload_method.reset(token)
        return ApiResult(res.get('code'), MessageSendResult(res.get('messageId')))

    @_catch
    async def recall(self,
                     message_id: MessageIdType,
                     ) -> ApiResult[None]:
        res: dict = await self._api.recall(target=message_id)
        return ApiResult(res.get('code'))

    @_catch
    async def get_message(self,
                          message_id: MessageIdType,
                          ) -> ApiResult[MessageEvent]:
        res: dict = await self._api.messageFromId(id=message_id, request_method='get')
        if res.get('code') == 0:
            return ApiResult(Code.Success, self.as_event(res['data']))
        else:
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
    async def get_group_member_list(self,
                                    group: ContactIdType,
                                    ) -> ApiResult[List[GroupMember]]:
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
    async def get_group_member_info(self,
                                    group: ContactIdType,
                                    qq: ContactIdType,
                                    ) -> ApiResult[GroupMember]:
        res = await self._api.memberInfo(target=group, memberId=qq, request_method='get')
        return ApiResult(Code.Success, GroupMember(
            id=qq,
            name=res.get('name'),
            permission=self._role_to_permission[res.get('permission')],
        ))

    @_catch
    async def set_group_mute(self,
                             group: ContactIdType,
                             qq: Optional[ContactIdType],
                             duration: Optional[int] = None,
                             ) -> ApiResult[None]:
        if qq:
            res = await self._api.mute(target=group, memberId=qq, time=duration, request_method='post')
            return ApiResult(res.get('code'))
        else:
            res = await self._api.muteAll(target=group, request_method='post')
            return ApiResult(res.get('code'))

    @_catch
    async def set_group_unmute(self,
                               group: ContactIdType,
                               qq: Optional[ContactIdType],
                               ) -> ApiResult[None]:
        if qq:
            res = await self._api.unmute(target=group, memberId=qq, request_method='post')
            return ApiResult(res.get('code'))
        else:
            res = await self._api.muteAll(target=group, request_method='post')
            return ApiResult(res.get('code'))

    @_catch
    async def get_friend_list(self) -> ApiResult[List[Friend]]:
        res = await self._api.friendList(request_method='get')
        friends = []
        for friend in res:
            friends.append(Friend(
                id=friend.get('id'),
                name=friend.get('nickname'),
                remark=friend.get('remark'),
            ))
        return ApiResult(Code.Success, friends)

    @_catch
    async def _fetch_message(self, count=100) -> ApiResult[List[Event]]:
        res = await self._api.fetchMessage(count=count, request_method='get')
        events = []
        if res.get('code') == 0:
            for ev in res.get('data', []):
                event = self.as_event(ev)
                if event:
                    events.append(event)
            return ApiResult(Code.Success, events)
        return ApiResult(res.get('code'))

    # Message Utils

    async def _upload_image(self, method, img: bytes, name: str):
        # return await self.call_action_(action="uploadImage", type=type_, img=img)
        res = await self._api.upload_image(filedata=img, filename=name, type=method)
        img_msg = Image(url=res['url'], id=res['imageId'], method=method)
        img_msg.referer = self.qq
        return img_msg

    async def _upload_voice(self, method, voice: bytes, name: str):
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
            logger.debug(f'Unknown message {msg} of type {type_}')
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
                # 'json': msg.json,
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
            logger.debug(f'Unknown message {msg} of type {type(msg)}')
            return {}

    def as_message_chain(self, chain: list) -> MessageChain:
        return MessageChain(filter(None, [self.as_message_el(x) for x in chain]))

    def as_mirai_chain(self, chain: MessageChain) -> list:
        return list(filter(None, [self.as_mirai_el(x) for x in chain]))

    _role_to_permission = {
        'OWNER': GroupPermission.OWNER,
        'ADMINISTRATOR': GroupPermission.ADMIN,
        'MEMBER': GroupPermission.MEMBER,
        None: GroupPermission.MEMBER,
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
                group=mi_event['member']['group']['id'],
                duration=mi_event['durationSeconds'],
            )
            return event
        elif type_ == 'MemberMuteEvent':
            event = GroupMuteEvent(
                qq=mi_event['member']['id'],
                operator=mi_event['operator']['id'] if mi_event['operator'] else self.qq,
                group=mi_event['member']['group']['id'],
                duration=mi_event['durationSeconds'],
            )
            return event
        elif type_ == 'BotUnmuteEvent':
            event = GroupUnmuteEvent(
                qq=self.qq,
                operator=mi_event['operator']['id'] if mi_event['operator'] else self.qq,
                group=mi_event['member']['group']['id'],
            )
            return event
        elif type_ == 'MemberUnmuteEvent':
            event = GroupUnmuteEvent(
                qq=mi_event['member']['id'],
                operator=mi_event['operator']['id'] if mi_event['operator'] else self.qq,
                group=mi_event['member']['group']['id'],
            )
            return event
        elif type_ == 'NewFriendRequestEvent':
            event = FriendAddRequestEvent(
                qq=mi_event['fromId'],
                comment=mi_event['message'],
                event_id=mi_event['eventId'],
            )
            return event
        elif type_ == 'MemberJoinRequestEvent':
            event = GroupJoinRequestEvent(
                qq=mi_event['fromId'],
                group=mi_event['groupId'],
                comment=mi_event['message'],
                event_id=mi_event['eventId'],
            )
            return event
        elif type_ == 'BotInvitedJoinGroupRequestEvent':
            event = GroupInvitedRequestEvent(
                operator=mi_event['fromId'],
                group=mi_event['groupId'],
                comment=mi_event['message'],
                event_id=mi_event['eventId'],
            )
            return event
        return None

    async def prepare_message(self, message: Message_T) -> MessageChain:
        # print('send: ', raw_message)
        if isinstance(message, MessageChain):
            message = await message.to(self, method=upload_method.get())
        else:
            message = await MessageChain(message).to(self, method=upload_method.get())
        # print('send now: ', raw_message)
        for msg in message:
            if isinstance(msg, Image):
                await msg.prepare(method=upload_method.get())
        # logger.debug(f'send now:  {raw_message}')
        return message

    async def relogin(self, sleep=5, retries=None):
        self._ok = False
        _retries = 0
        while not retries or _retries < retries:
            _retries += 1
            try:
                new_session_key = await self._auth()
                if not new_session_key:
                    raise ApiError(Code.Unavailable)

                self._session_key = new_session_key
                self._api.update_session(new_session_key)

                if await self._api.verify(qq=self.qq):
                    if self._enable_ws:
                        asyncio.create_task(self.set_report_ws())
                    return True
                else:
                    raise ApiError(Code.Unavailable, "Verify failed")
            except ApiError as e:
                logger.info(f'Failed to login by {e}, retry in {sleep}s or exit ...')
                await asyncio.sleep(sleep)

    def set_report(self, report_path: str, **kwargs):

        @self._app.route(report_path, methods=['POST'])
        async def _on_report():
            event = await quart.request.get_json()
            logger.debug(event)
            try:
                event = self.as_event(event)
                if event:
                    self.handle_event_nowait(event)
            except Exception as e:
                logger.critical(e)
                return {'code': -1}

            return {'code': 0}

    def set_report_ws(self, **kwargs):
        async def _ws():
            async with aiohttp.ClientSession() as client:
                async with client.ws_connect(f"{self._api_root}all/?sessionKey={self._session_key}", **kwargs) as ws:
                    while True:
                        try:
                            event = await ws.receive_json()
                        except TypeError:
                            if ws.closed:
                                logger.warning('Websocket closed, try relogin')
                                await self.relogin()
                                return
                            logger.error(f"TypeError in parsing ws event")
                            continue
                        if not event:
                            continue

                        logger.debug(event)
                        try:
                            event = self.as_event(event)
                            if event:
                                self.handle_event_nowait(event)
                        except Exception as e:
                            logger.critical(e)

        return _ws()

    def set_poll(self, interval=0.5):
        async def _poll():
            while True:
                try:
                    res = await self._fetch_message()
                    if res.ok:
                        for event in res.data:
                            self.handle_event_nowait(event)
                except Exception as e:
                    logger.error(e)
                finally:
                    await asyncio.sleep(interval)

        return _poll()
