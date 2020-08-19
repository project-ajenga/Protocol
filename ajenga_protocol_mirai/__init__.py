import asyncio
import json
from typing import Optional, Dict, List
from urllib.parse import urlparse
from urllib.request import url2pathname
import contextvars

import aiofiles
import aiohttp
import quart
from quart import Quart

from ajenga.log import logger

import ajenga.models.message as raw_message
from ajenga.models.contact import Group, GroupMember
from ajenga.models.message import MessageChain, MessageElement, Message_T
from ajenga.models.event_impl import MessageEvent, GroupPermission, Sender, \
    GroupMuteEvent, GroupRecallEvent, GroupUnmuteEvent, GroupMessageEvent, FriendMessageEvent, TempMessageEvent
from ajenga.protocol import Api
from ajenga.protocol.api import ApiResult, MessageSendResult
from ajenga_app import BotSession


logger = logger.getChild('mirai-protocol')

from . import api


upload_method = contextvars.ContextVar('upload_method')
METHOD_GROUP = 'group'
METHOD_FRIEND = 'friend'
METHOD_TEMP = 'temp'


class Source(raw_message.Meta):
    def __init__(self, id_):
        super(Source, self).__init__()
        self.id_ = id_


class Image(raw_message.Image):
    def __init__(self, *, url=None, content=None, id_=None, type_=None):
        super(Image, self).__init__(url=url, content=content)
        self.type_ = type_ or upload_method.get()
        self.id_ = id_
        if not id_ and type_:
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
        img = await self.referer._upload_image(self.type_, self.content, f'{self.hash_}.png')
        logger.debug(f'Got resp !  {img}')
        self.id_ = img.id_

    async def prepare(self, type_=None):
        if self.id_:
            return
        elif type_ and type_ != self.type_:
            if self.task:
                logger.warning(f"Unused image upload cache {self.type_} {type_}")
            self.type_ = type_
            return await self._prepare()
        elif self.task:
            await self.task
        else:
            raise ValueError("No upload method specified for Image!")

    def raw(self) -> "MessageElement":
        return raw_message.Image(url=self.url, content=self.content)

    def __eq__(self, other):
        return isinstance(other, Image) and self.id_ == other.id_


class Quote(raw_message.Quote):
    def __init__(self, id_, origin):
        super(Quote, self).__init__(id_=id_, origin=origin)

    def raw(self) -> "MessageElement":
        return raw_message.Quote(id_=self.id_)


class MiraiSession(BotSession, Api):
    def __init__(self, api_root, session_key, qq):
        self._qq = qq
        self._api = api.HttpApi(api_root, session_key, 30)

    @property
    def qq(self) -> int:
        return self._qq

    # Implement abstract function for BotSession

    @property
    def api(self) -> Api:
        return self

    def wrap_message(self, message: MessageElement, method=None) -> MessageElement:
        if message.referer == self:
            return message
        message = message.raw()
        if isinstance(message, raw_message.Image):
            message2 = Image(url=message.url, content=message.content, type_=method)
            message2.referer = self
            return message2
        else:
            message.referer = self
            return message

    # Implement abstract function for Api

    async def send_group_message(self, group: int, message: Message_T) -> ApiResult[MessageSendResult]:
        token = upload_method.set(METHOD_GROUP)
        msg = await self.prepare_message(message)
        if quote := msg.get_first(Quote):
            msg.remove(quote)
            res: dict = await self._api.sendGroupMessage(group=group, messageChain=self.as_mirai_chain(msg), quote=quote.id_)
        else:
            res: dict = await self._api.sendGroupMessage(group=group, messageChain=self.as_mirai_chain(msg))
        upload_method.reset(token)
        return ApiResult(res.get('code'), res.get('message'), res.get('messageId'))

    async def send_friend_message(self, qq: int, message: Message_T) -> ApiResult[MessageSendResult]:
        token = upload_method.set(METHOD_FRIEND)
        msg = await self.prepare_message(message)
        res: dict = await self._api.sendFriendMessage(qq=qq, messageChain=self.as_mirai_chain(msg))
        upload_method.reset(token)
        return ApiResult(res.get('code'), res.get('message'), res.get('messageId'))

    async def send_temp_message(self, qq: int, group: int, message: Message_T) -> ApiResult[MessageSendResult]:
        token = upload_method.set(METHOD_TEMP)
        msg = await self.prepare_message(message)
        res: dict = await self._api.sendTempMessage(qq=qq, group=group, messageChain=self.as_mirai_chain(msg))
        upload_method.reset(token)
        return ApiResult(res.get('code'), res.get('message'), res.get('messageId'))

    async def get_group_list(self) -> ApiResult[List[Group]]:
        res = await self._api.groupList(request_method='get')
        groups = []
        for g in res:
            groups.append(Group(
                id_=g.get('id'),
                name=g.get('name'),
                permission=self._role_to_permission[g.get('permission')],
            ))
        return ApiResult(0, 'success', groups)

    async def get_group_member_list(self, group: int) -> ApiResult[List[GroupMember]]:
        res = await self._api.memberList(target=group, request_method='get')
        members = []
        for member in res:
            members.append(GroupMember(
                id_=member.get('id'),
                name=member.get('memberName'),
                permission=self._role_to_permission[member.get('permission')],
            ))
        return ApiResult(0, 'status', members)

    async def get_group_member_info(self, group: int, qq: int) -> ApiResult[GroupMember]:
        res = await self._api.memberInfo(target=group, memberId=qq, request_method='get')
        return ApiResult(0, 'success', GroupMember(
            id_=res.get('id'),
            name=res.get('memberName'),
            permission=self._role_to_permission[res.get('permission')],
        ))

    # Message Utils

    async def _upload_image(self, type_, img: bytes, name: str):
        # return await self.call_action_(action="uploadImage", type=type_, img=img)
        res = await self._api.upload(filedata=img, filename=name, type=type_, img=img, )
        img_msg = Image(url=res['url'], id_=res['imageId'], type_=type_)
        img_msg.referer = self
        return img_msg

    # Message Transition

    def as_message_el(self, msg: dict) -> MessageElement:
        # print(msg)
        type_: str = msg['type']
        if type_ == 'Source':
            ret = Source(id_=msg['id'])
        elif type_ == 'Plain':
            ret = raw_message.Plain(text=msg['text'])
        elif type_ == 'Quote':
            ret = Quote(id_=msg['id'], origin=self.as_message_chain(msg['origin']))
        elif type_ == 'At':
            ret = raw_message.At(qq=msg['target'])
        elif type_ == 'AtAll':
            ret = raw_message.AtAll()
        elif type_ == 'Face':
            ret = raw_message.Face(id_=msg['faceId'])
        elif type_ == 'Image':
            ret = Image(url=msg['url'], id_=msg['imageId'])
        elif type_ == 'Voice':
            ret = raw_message.Voice(url=msg['url'])
        elif type_ == 'App':
            ret = raw_message.App(content=json.loads(msg['content']))
        else:
            ret = raw_message.Unknown()
        ret.referer = self
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
                'imageId': msg.id_,
            }
        elif isinstance(msg, raw_message.Face):
            return {
                'type': 'Face',
                'faceID': msg.id_,
            }
        elif isinstance(msg, raw_message.Voice):
            return {
                'type': 'Voice',
                'url': msg.url
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
                message_id=msg[0].id_,
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
                message_id=msg[0].id_,
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
                message_id=msg[0].id_,
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
                operator=mi_event['operator']['id'],
            )
            return event
        elif type_ == 'BotMuteEvent':
            event = GroupMuteEvent(
                qq=None,
                operator=mi_event['operator']['id'],
                duration=mi_event['durationSeconds'],
            )
            return event
        elif type_ == 'MemberMuteEvent':
            event = GroupMuteEvent(
                qq=mi_event['member']['id'],
                operator=mi_event['operator']['id'],
                duration=mi_event['durationSeconds'],
            )
            return event
        elif type_ == 'BotUnmuteEvent':
            event = GroupUnmuteEvent(
                qq=None,
                operator=mi_event['operator']['id'],
            )
            return event
        elif type_ == 'MemberUnmuteEvent':
            event = GroupUnmuteEvent(
                qq=mi_event['member']['id'],
                operator=mi_event['operator']['id'],
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
                await msg.prepare(type_=upload_method.get())
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


async def connect(host, port, auth_key, qq) -> MiraiSession:
    api_root = f'http://{host}:{port}/'

    async def auth():
        async with aiohttp.request("POST", api_root + 'auth', json={'authKey': auth_key}) as resp:
            if 200 <= resp.status < 300:
                result = json.loads(await resp.text())
                logger.info(f'Login Mirai: {result}')
                if result.get('code') == 0:
                    return result['session']

    session_key = await auth()

    session = MiraiSession(api_root, session_key, qq)
    if await session._api.verify(qq=qq):
        return session
