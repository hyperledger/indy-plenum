from plenum.common.messages.fields import NonNegativeNumberField, NonEmptyStringField, AnyValueField, IterableField, \
    MapField
from plenum.common.messages.message_base import MessageBase


class Message1(MessageBase):
    typename = 'Message1'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', NonEmptyStringField()),
    )


class Message2(MessageBase):
    typename = 'Message2'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', AnyValueField()),
    )


class Message3(MessageBase):
    typename = 'Message3'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', IterableField(AnyValueField())),
    )


class Message4(MessageBase):
    typename = 'Message4'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', MapField(AnyValueField(), AnyValueField())),
    )

class SomeNonMessageClass:
    typename = 'SomeNonMessageClass'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', NonEmptyStringField()),
    )
