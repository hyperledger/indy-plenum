import pytest

from plenum.common.messages.fields import NonNegativeNumberField
from plenum.common.messages.message_base import MessageBase


class MessageTest(MessageBase):
    typename = 'MessageTest'
    schema = (
        ('a', NonNegativeNumberField()),
        ('b', NonNegativeNumberField()),
    )


def test_init_args():
    msg = MessageTest(1, 2)
    assert msg
    assert msg.a == 1
    assert msg.b == 2


def test_init_kwargs():
    msg1 = MessageTest(a=1, b=2)
    assert msg1
    assert msg1.a == 1
    assert msg1.b == 2


def test_message_base_init_with_both_args_kwargs():
    with pytest.raises(ValueError) as excinfo:
        MessageTest(1, b=3)
    assert "*args, **kwargs cannot be used together" == str(excinfo.value)
