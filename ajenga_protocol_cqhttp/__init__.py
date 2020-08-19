import asyncio
import functools
from typing import Callable, Awaitable, Union, Any
import json
import aiocqhttp
import aiocqhttp.api
from aiocqhttp import CQHttp, message as cq_message

from ajenga.log import logger
import ajenga.models.message as raw_message
from ajenga.models.message import MessageChain, MessageElement, Message_T
from ajenga.models.event_impl import MessageEvent, GroupPermission, Sender, \
    GroupMuteEvent, GroupRecallEvent, GroupUnmuteEvent, GroupMessageEvent, FriendMessageEvent, TempMessageEvent
from ajenga.protocol import Api
from ajenga_app import BotSession

logger = logger.getChild('cqhttp-protocol')


class Image(raw_message.Image):
    def __init__(self, *, url=None, content=None, file=None):
        super(Image, self).__init__(url=url, content=content)
        self.file = file

    def raw(self) -> "MessageElement":
        return raw_message.Image(url=self.url, content=self.content)


class Voice(raw_message.Voice):
    def __init__(self, *, url=None, content=None, file=None):
        super(Voice, self).__init__(url=url, content=content)
        self.file = file
        # if self.file and not self.url and not self.content:
            # self._task = asyncio.create_task(self._prepare())
    #
    # async def _prepare(self):
    #     url =

    def raw(self) -> "MessageElement":
        return raw_message.Image(url=self.url, content=self.content)


class CQSession(BotSession, Api):
    _cqhttp: CQHttp

    def __init__(self, qq, **kwargs):
        self._qq = qq
        self._cqhttp = CQHttp(message_class=aiocqhttp.Message, **kwargs)
        self._api = self._cqhttp.api
        self._queue = asyncio.Queue()
        self._open = True

        @self._cqhttp.on_message()
        async def _on_message(event: aiocqhttp.Event):
            logger.debug(event)
            if event := self.as_event(event):
                self.handle_event_nowait(event)

        @self._cqhttp.on_request()
        @self._cqhttp.on_notice()
        async def _on_message(event: aiocqhttp.Event):
            logger.debug(event)

    def run_task(self, **kwargs):
        return self._cqhttp.run_task(use_reloader=False, **kwargs)

    @property
    def qq(self) -> int:
        return self._qq

    @property
    def api(self) -> Api:
        return self

    def wrap_message(self, message: MessageElement) -> MessageElement:
        if message.referer == self:
            return message
        elif isinstance(message, raw_message.Image):
            message = message.raw()
            message = Image(url=message.url, content=message.content)
            message.referer = self
            return message
        else:
            message = message.raw()
            message.referer = self
            return message

    #

    async def send_group_message(self, group: int, message: Message_T):
        return await self._api.send_group_msg(group_id=group, message=self.as_cq_chain(message))

    async def send_friend_message(self, qq: int, message: Message_T):
        return await self._api.send_private_msg(user_id=qq, message=self.as_cq_chain(message))

    async def send_temp_message(self, qq: int, group: int, message: Message_T):
        return await self._api.send_private_msg(user_id=qq, message=self.as_cq_chain(message))

    #

    def as_cq_el(self, message: MessageElement) -> cq_message.MessageSegment:
        if isinstance(message, raw_message.Plain):
            return cq_message.MessageSegment.text(message.text)
        elif isinstance(message, raw_message.At):
            return cq_message.MessageSegment.at(message.target)
        elif isinstance(message, raw_message.AtAll):
            return cq_message.MessageSegment(type_='at', data={'qq': 'all'})
        elif isinstance(message, raw_message.Face):
            return cq_message.MessageSegment.face(message.id_)
        elif isinstance(message, Image) and message.file:
            return cq_message.MessageSegment.image(message.file)
        elif isinstance(message, raw_message.Image):
            return cq_message.MessageSegment.image(message.url)
        elif isinstance(message, Voice):
            return cq_message.MessageSegment.record(message.file)
        elif isinstance(message, raw_message.Voice):
            return cq_message.MessageSegment.record(message.url)
        else:
            return cq_message.MessageSegment.text('')

    def as_cq_chain(self, message: Message_T) -> str:
        ret = ''.join(str(self.as_cq_el(x)) for x in MessageChain(message))
        logger.debug(f'sending : {repr(ret)}')
        return ret

    def as_message_el(self, message: aiocqhttp.MessageSegment) -> MessageElement:
        type_, data = message.type, message.data
        if type_ == 'text':
            ret = raw_message.Plain(data['text'])
        elif type_ == 'face':
            ret = raw_message.Face(int(data['id_']))
        elif type_ == 'image':
            ret = Image(url=data['url'], file=data['file'])
        elif type_ == 'at':
            if data['qq'] == 'all':
                ret = raw_message.AtAll()
            else:
                ret = raw_message.At(int(data['qq']), display='@')
        elif type_ == 'record':
            ret = Voice(file=data['file'])
        elif type_ == 'rich':
            ret = raw_message.App(content=json.loads(aiocqhttp.message.unescape(data['content'])))
        else:
            ret = raw_message.Unknown()
        # else:
        #     return Plain(str(message))
        ret.referer = self
        return ret

    def as_message_chain(self, message: aiocqhttp.Message) -> MessageChain:
        return MessageChain(list(self.as_message_el(x) for x in message))

    _role_to_permission = {
        'owner': GroupPermission.OWNER,
        'admin': GroupPermission.ADMIN,
        'member': GroupPermission.MEMBER,
        None: GroupPermission.NONE,
    }

    def as_event(self, cq_event: aiocqhttp.Event):
        if cq_event.type == 'message':
            if cq_event.detail_type == 'group':
                msg = self.as_message_chain(cq_event.message)
                event = GroupMessageEvent(
                    message=msg,
                    message_id=cq_event.message_id,
                    group=cq_event.group_id,
                    sender=Sender(
                        qq=cq_event.sender['user_id'],
                        name=cq_event.sender.get('nickname'),
                        permission=GroupPermission(self._role_to_permission.get(cq_event.sender.get('role'))),
                    ),
                )
                return event
            elif cq_event.detail_type == 'friend':
                msg = self.as_message_chain(cq_event.message)
                event = FriendMessageEvent(
                    message=msg,
                    message_id=cq_event.message_id,
                    sender=Sender(
                        qq=cq_event.sender['user_id'],
                        name=cq_event.sender.get('nickname'),
                    ),
                )
                return event
        elif cq_event.type == 'notice':
            if cq_event.detail_type == 'group_ban':
                if cq_event['duration']:
                    event = GroupMuteEvent(
                        qq=cq_event.user_id,
                        operator=cq_event.operator_id,
                        duration=cq_event['duration'],
                    )
                    return event
                else:
                    event = GroupUnmuteEvent(
                        qq=cq_event.user_id,
                        operator=cq_event.operator_id,
                    )
                    return event
        return None


def connect(qq, **kwargs):
    session = CQSession(qq=qq)
    asyncio.create_task(session.run_task(**kwargs))
    return session
