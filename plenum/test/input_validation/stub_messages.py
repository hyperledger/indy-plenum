from plenum.common.messages.fields import NonNegativeNumberField, NonEmptyStringField, AnyValueField, IterableField, \
    MapField, HexField, BooleanField
from plenum.common.messages.message_base import MessageBase, NetworkMessage


class Message1MsgData(MessageBase):
    typename = 'Message1'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', NonEmptyStringField()),
    )


class Message1(NetworkMessage):
    msg_data_cls = Message1MsgData


class Message2MsgData(MessageBase):
    typename = 'Message2'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', AnyValueField()),
    )


class Message2(NetworkMessage):
    msg_data_cls = Message2MsgData


class Message3MsgData(MessageBase):
    typename = 'Message3'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', IterableField(BooleanField())),
    )


class Message3(NetworkMessage):
    msg_data_cls = Message3MsgData


class Message4MsgData(MessageBase):
    typename = 'Message4'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', MapField(HexField(), HexField())),
    )


class Message4(NetworkMessage):
    msg_data_cls = Message4MsgData


class SomeNonMessageClassMsgData:
    typename = 'SomeNonMessageClass'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', NonEmptyStringField()),
    )


class SomeNonMessageClass(NetworkMessage):
    msg_data_cls = SomeNonMessageClassMsgData
