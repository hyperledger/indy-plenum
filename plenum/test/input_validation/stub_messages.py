from plenum.common.messages.fields import NonNegativeNumberField, NonEmptyStringField, AnyValueField
from plenum.common.messages.message_base import MessageBase


class AMessage1(MessageBase):
    typename = 'AMessage1'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', NonEmptyStringField()),
    )


class AMessage2(MessageBase):
    typename = 'AMessage2'
    schema = (
        ('a', NonNegativeNumberField()),
        ('c', AnyValueField()),
    )


class SomeNonMessageClass:
    typename = 'SomeNonMessageClass'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', NonEmptyStringField()),
    )
