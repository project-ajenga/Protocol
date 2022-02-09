import asyncio
import base64
import json
from dataclasses import dataclass, field
from functools import wraps
from typing import List, Optional

import aiocqhttp
import aiocqhttp.api
import ajenga.event as raw_event
import ajenga.message as raw_message
from aiocqhttp import CQHttp
from aiocqhttp import message as cq_message
from ajenga.app import BotSession, get_session, register_session
from ajenga.ctx import this
from ajenga.event import (Event, EventType, FriendMessageEvent,
                          FriendRecallEvent, GroupJoinEvent, GroupLeaveEvent,
                          GroupMessageEvent, GroupMuteEvent, GroupPermission,
                          GroupRecallEvent, GroupUnmuteEvent, MessageEvent,
                          Sender, TempMessageEvent)
from ajenga.log import logger
from ajenga.message import (Message_T, MessageChain, MessageElement,
                            MessageIdType)
from ajenga.models import ContactIdType, Friend, Group, GroupMember
from ajenga.protocol import Api, ApiResult, Code, MessageSendResult

logger = logger.getChild('protocol.cqhttp')


@dataclass
class Raw(raw_message.Meta):
    type: str = 'raw'
    data: dict = field(default_factory=dict)


@dataclass
class Image(raw_message.Image):
    __struct_type__ = "cq:image"
    __struct_fields__ = ("url", "hash", "file")

    file: str = None

    def __post_init__(self):
        if self.file and not self.hash:
            self.hash = self.file[:self.file.index('.')]
        if not self.url and not self.file and self.content:
            self.url = self.base64()

    def base64(self) -> str:
        base64_str = base64.b64encode(self.content).decode()
        return 'base64://' + base64_str

    async def raw(self) -> Optional[MessageElement]:
        return raw_message.Image(url=self.url, hash=self.hash, content=self.content)

    def __eq__(self, other):
        return (isinstance(other, Image) and self.file == other.file) or super(Image, self).__eq__(other)


@dataclass
class Voice(raw_message.Voice):
    file: str = None

    async def raw(self) -> "MessageElement":
        return raw_message.Voice(url=self.url, content=self.content)


@dataclass
class Quote(raw_message.Quote):
    async def raw(self) -> Optional[MessageElement]:
        return raw_message.Quote(id=self.id)


_ALLOW_RETRY = False


def set_allow_retry(allow_retry: bool):
    global _ALLOW_RETRY
    _ALLOW_RETRY = allow_retry


def _catch(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            if (not _ALLOW_RETRY):
                return ApiResult(Code.Unspecified, message=str(e))

            logger.info("Retrying...")
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.exception(e)
                logger.error("Retrying failed")
                return ApiResult(Code.Unspecified, message=str(e))

    return wrapper


@dataclass
class FriendAddRequestEvent(raw_event.FriendAddRequestEvent):
    flag: str

    @_catch
    async def _reply(self, approve: bool):
        session: CQSession = this.bot
        await session._api.set_friend_add_request(flag=self.flag, approve=approve)

    async def accept(self, **kwargs):
        return await self._reply(True)

    async def reject(self, **kwargs):
        return await self._reply(False)

    async def ignore(self):
        pass


@dataclass
class GroupJoinRequestEvent(raw_event.GroupJoinRequestEvent):
    flag: str

    @_catch
    async def _reply(self, approve: bool):
        session: CQSession = this.bot
        await session._api.set_group_add_request(flag=self.flag, sub_type='add', approve=approve)

    async def accept(self, **kwargs):
        return await self._reply(True)

    async def reject(self, **kwargs):
        return await self._reply(False)

    async def ignore(self):
        pass


@dataclass
class GroupInvitedRequestEvent(raw_event.GroupInvitedRequestEvent):
    flag: str

    @_catch
    async def _reply(self, approve: bool):
        session: CQSession = this.bot
        await session._api.set_group_add_request(flag=self.flag, sub_type='invite', approve=approve)

    async def accept(self, **kwargs):
        return await self._reply(True)

    async def reject(self, **kwargs):
        return await self._reply(False)

    async def ignore(self):
        pass


@dataclass
class RawCQEvent(Event):
    type: EventType = field(default=EventType.Protocol, init=False)
    protocol: str
    event: dict


class CQProtocol:
    _cqhttp: CQHttp

    def __init__(self, **kwargs):
        self._cqhttp = CQHttp(message_class=aiocqhttp.Message, **kwargs)
        self._api = self._cqhttp.api

        @self._cqhttp.on_message()
        @self._cqhttp.on_request()
        @self._cqhttp.on_notice()
        async def _on_event(event: aiocqhttp.Event):
            session: CQSession = get_session(event.self_id)
            if not session or not isinstance(session, CQSession):
                return
            logger.debug(f"[event] {event}")
            # For CQ Compact:
            session.handle_event_nowait(RawCQEvent(protocol="cqhttp", event=event))

            event = session.as_event(event)
            if event:
                session.handle_event_nowait(event)

        @self._cqhttp.on_meta_event("lifecycle.connect")
        async def _on_meta_event(event: aiocqhttp.Event):
            session = CQSession(qq=event.self_id, cq=self._cqhttp)
            register_session(session, event.self_id)

    def run_task(self, **kwargs):
        return self._cqhttp.run_task(use_reloader=False, **kwargs)

class CQSession(BotSession, Api):

    def __init__(self, qq: ContactIdType, cq: CQHttp):
        self._qq = qq
        self._api = cq.api

    @property
    def qq(self) -> int:
        return self._qq

    @property
    def ok(self) -> bool:
        return True

    @property
    def api(self) -> Api:
        return self

    async def wrap_message(self, message: MessageElement, **kwargs) -> MessageElement:
        if message.referer == self.qq:
            return message
        message = await message.raw()
        if message is None:
            return None
        elif isinstance(message, raw_message.Image):
            message = Image(url=message.url, content=message.content)
            message.referer = self.qq
            return message
        else:
            message.referer = self.qq
            return message

    #

    def _as_cq_forward_nodes(self, message: raw_message.Forward) -> List:
        """
        Go-CQHTTP Forward Message Support
        Experimental
        """
        ret = []
        for node in message.content:
            if node.id:
                ret.append({
                    'type': 'node',
                    'data': {
                        'id': node.id,
                    },
                })
            elif node.qq:
                ret.append({
                    'type': 'node',
                    'data': {
                        'name': node.name,
                        'uin': node.qq,
                        'content': self.as_cq_chain(node.content),
                    },
                })
        logger.debug(repr(ret))
        return ret

    @_catch
    async def send_group_message(self,
                                 group: ContactIdType,
                                 message: Message_T,
                                 ) -> ApiResult[MessageSendResult]:
        message = await self.prepare_message(message)
        # Go-CQHTTP Forward Message
        if message.get_first(raw_message.Forward):
            res: dict = await self._api.send_group_forward_msg(group_id=group, messages=self._as_cq_forward_nodes(message.get_first(raw_message.Forward)))
        else:
            res: dict = await self._api.send_group_msg(group_id=group, message=self.as_cq_chain(message))
        return ApiResult(Code.Success, MessageSendResult(res['message_id']))

    @_catch
    async def send_friend_message(self,
                                  qq: ContactIdType,
                                  message: Message_T,
                                  ) -> ApiResult[MessageSendResult]:
        message = await self.prepare_message(message)
        res: dict = await self._api.send_private_msg(user_id=qq, message=self.as_cq_chain(message))
        return ApiResult(Code.Success, MessageSendResult(res['message_id']))

    @_catch
    async def send_temp_message(self,
                                qq: ContactIdType,
                                group: ContactIdType,
                                message: Message_T,
                                ) -> ApiResult[MessageSendResult]:
        message = await self.prepare_message(message)
        res: dict = await self._api.send_private_msg(user_id=qq, message=self.as_cq_chain(message))
        return ApiResult(Code.Success, MessageSendResult(res['message_id']))

    @_catch
    async def recall(self,
                     message_id: MessageIdType,
                     ) -> ApiResult[None]:
        res: dict = await self._api.delete_msg(message_id=message_id)
        return ApiResult(Code.Success)

    # @_catch
    # async def get_message(self,
    #                       message_id: MessageIdType,
    #                       ) -> ApiResult[MessageEvent]:
    #     res: dict = await self._api.get_msg(message_id=message_id)
    #     logger.debug(f'get message res: {res}')
    #     return ApiResult(Code.Unavailable)

    @_catch
    async def get_group_list(self) -> ApiResult[List[Group]]:
        res = await self._api.get_group_list()
        groups = []
        for g in res:
            groups.append(Group(
                id=g.get('group_id'),
                name=g.get('group_name'),
                permission=GroupPermission.NONE,
            ))
        return ApiResult(Code.Success, groups)

    @_catch
    async def get_group_member_list(self,
                                    group: ContactIdType,
                                    ) -> ApiResult[List[GroupMember]]:
        res = await self._api.get_group_member_list(group_id=group)
        members = []
        for member in res:
            members.append(GroupMember(
                id=member.get('user_id'),
                name=member.get('card') or member.get('nickname'),
                permission=self._role_to_permission[member.get('role')],
            ))
        return ApiResult(Code.Success, members)

    @_catch
    async def get_group_member_info(self,
                                    group: ContactIdType,
                                    qq: ContactIdType,
                                    ) -> ApiResult[GroupMember]:
        res = await self._api.get_group_member_info(group_id=group, user_id=qq)
        return ApiResult(Code.Success, GroupMember(
            id=res.get('user_id'),
            name=res.get('nickname'),
            permission=self._role_to_permission[res.get('role')],
        ))

    @_catch
    async def set_group_kick(self,
                             group: ContactIdType,
                             qq: ContactIdType,
                             ) -> ApiResult[None]:
        await self._api.set_group_kick(group_id=group, user_id=qq)
        return ApiResult(Code.Success)

    @_catch
    async def set_group_leave(self,
                              group: ContactIdType,
                              ) -> ApiResult[None]:
        await self._api.set_group_leave(group_id=group)
        return ApiResult(Code.Success)

    @_catch
    async def set_group_mute(self,
                             group: ContactIdType,
                             qq: Optional[ContactIdType],
                             duration: Optional[int] = None,
                             ) -> ApiResult[None]:
        if qq:
            await self._api.set_group_ban(group_id=group, user_id=qq, duration=duration)
        else:
            await self._api.set_group_whole_ban(group_id=group, enable=True)
        return ApiResult(Code.Success)

    @_catch
    async def set_group_unmute(self,
                               group: ContactIdType,
                               qq: Optional[ContactIdType],
                               ) -> ApiResult[None]:
        if qq:
            await self._api.set_group_ban(group_id=group, user_id=qq, duration=0)
        else:
            await self._api.set_group_whole_ban(group_id=group, enable=False)
        return ApiResult(Code.Success)

    @_catch
    async def get_friend_list(self) -> ApiResult[List[Friend]]:
        res = await self._api.get_friend_list()
        friends = []
        for friend in res:
            friends.append(Friend(
                id=friend.get('user_id'),
                name=friend.get('nickname'),
                remark=friend.get('remark'),
            ))
        return ApiResult(Code.Success, friends)

    #

    async def prepare_message(self, message: Message_T) -> MessageChain:
        return await MessageChain(message).to(self)

    def as_cq_el(self, message: MessageElement) -> cq_message.MessageSegment:
        if isinstance(message, Raw):
            return cq_message.MessageSegment(type_=message.type, data=message.data)
        elif isinstance(message, raw_message.Plain):
            return cq_message.MessageSegment.text(message.text)
        elif isinstance(message, raw_message.At):
            return cq_message.MessageSegment.at(message.target)
        elif isinstance(message, raw_message.AtAll):
            return cq_message.MessageSegment(type_='at', data={'qq': 'all'})
        elif isinstance(message, raw_message.Face):
            return cq_message.MessageSegment.face(message.id)
        elif isinstance(message, Image) and message.file:
            return cq_message.MessageSegment.image(message.file)
        elif isinstance(message, raw_message.Image):
            return cq_message.MessageSegment.image(message.url)
        elif isinstance(message, Voice):
            return cq_message.MessageSegment.record(message.file)
        elif isinstance(message, raw_message.Voice):
            return cq_message.MessageSegment.record(message.url)
        elif isinstance(message, raw_message.App):
            return cq_message.MessageSegment(type_='json',
                                             data={'data': cq_message.escape((json.dumps(message.content)))})
        elif isinstance(message, raw_message.Xml):
            return cq_message.MessageSegment(type_='xml',
                                             data={'data': cq_message.escape(message.content)})
        else:
            logger.debug(f'Unknown message {message} of type {type(message)}')
            return cq_message.MessageSegment.text('')

    def as_cq_chain(self, message: Message_T) -> List:
        ret = list((self.as_cq_el(x)) for x in MessageChain(message))
        return ret

    def as_cq_chain_string(self, message: Message_T) -> str:
        ret = ''.join(str(self.as_cq_el(x)) for x in MessageChain(message))
        return ret

    def as_message_el(self, message: aiocqhttp.MessageSegment) -> MessageElement:
        type_, data = message.type, message.data
        if type_ == 'text':
            ret = raw_message.Plain(data['text'])
        elif type_ == 'face':
            ret = raw_message.Face(int(data['id']))
        elif type_ == 'image':
            ret = Image(url=data['url'], file=data['file'])
        elif type_ == 'at':
            if data['qq'] == 'all':
                ret = raw_message.AtAll()
            else:
                ret = raw_message.At(int(data['qq']))
        elif type_ == 'record':
            ret = Voice(file=data['file'])
        elif type_ == 'rich':
            ret = raw_message.App(content=json.loads(cq_message.unescape(data['content'])))
        elif type_ == 'reply':
            ret = Quote(id=int(data['id']))
        elif type_ == 'json':
            ret = raw_message.App(content=json.loads(cq_message.unescape(data['data'])))
        elif type_ == 'xml':
            ret = raw_message.Xml(content=cq_message.unescape(data['data']))
        else:
            logger.debug(f'Unknown message {message} of type {type_}')
            ret = raw_message.Unknown()
        ret.referer = self.qq
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
            elif cq_event.detail_type == 'private' and cq_event.sub_type == 'friend':
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
            elif cq_event.detail_type == 'private' and cq_event.sub_type == 'group':
                msg = self.as_message_chain(cq_event.message)
                event = TempMessageEvent(
                    message=msg,
                    message_id=cq_event.message_id,
                    sender=Sender(
                        qq=cq_event.sender['user_id'],
                        name=cq_event.sender.get('nickname'),
                    ),
                    group=0
                )
                return event
        elif cq_event.type == 'notice':
            if cq_event.detail_type == 'group_ban':
                if cq_event['duration']:
                    event = GroupMuteEvent(
                        qq=cq_event.user_id,
                        operator=cq_event.operator_id,
                        group=cq_event.group_id,
                        duration=cq_event['duration'],
                    )
                    return event
                else:
                    event = GroupUnmuteEvent(
                        qq=cq_event.user_id,
                        operator=cq_event.operator_id,
                        group=cq_event.group_id,
                    )
                    return event
            elif cq_event.detail_type == 'group_recall':
                event = GroupRecallEvent(
                    qq=cq_event.user_id,
                    operator=cq_event.operator_id,
                    group=cq_event.group_id,
                    message_id=cq_event.message_id,
                )
                return event
            elif cq_event.detail_type == 'group_increase':
                event = GroupJoinEvent(
                    qq=cq_event.user_id,
                    operator=cq_event.operator_id,
                    group=cq_event.group_id,
                )
                return event
            elif cq_event.detail_type == 'group_decrease':
                event = GroupLeaveEvent(
                    qq=cq_event.user_id,
                    operator=cq_event.operator_id,
                    group=cq_event.group_id,
                )
                return event
            elif cq_event.detail_type == 'friend_recall':
                event = FriendRecallEvent(
                    qq=cq_event.user_id,
                    message_id=cq_event.message_id,
                )
                return event
        elif cq_event.type == 'request':
            if cq_event.detail_type == 'friend':
                event = FriendAddRequestEvent(
                    qq=cq_event.user_id,
                    comment=cq_event.comment,
                    flag=cq_event.flag,
                )
                return event
            elif cq_event.detail_type == 'group' and cq_event.sub_type == 'add':
                event = GroupJoinRequestEvent(
                    qq=cq_event.user_id,
                    group=cq_event.group_id,
                    comment=cq_event.comment,
                    flag=cq_event.flag,
                )
                return event
            elif cq_event.detail_type == 'group' and cq_event.sub_type == 'invite':
                event = GroupInvitedRequestEvent(
                    operator=cq_event.user_id,
                    group=cq_event.group_id,
                    comment=cq_event.comment,
                    flag=cq_event.flag,
                )
                return event
        return None
