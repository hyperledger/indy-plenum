import pytest

from plenum.common.messages.message_base import MessageBase

class MessageTest(MessageBase):
    typename = 'MessageTest'
    schema = (
        ('a', {}),
        ('b', {}),
    )

def test_message_base_init_with_both_args_kwargs():
    with pytest.raises(ValueError) as excinfo:
         MessageTest(1, b=3)
    assert "*args, **kwargs cannot be used together" == str(excinfo.value)

def test_message_base_init_with_invalid_args_number():
    with pytest.raises(ValueError) as excinfo:
         MessageTest(1, 2, 3)
    assert ("should be less than or equal to the number of fields "
            "in schema {}".format(len(MessageTest.schema))) in str(excinfo.value)
