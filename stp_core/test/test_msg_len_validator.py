import pytest
from plenum.common.exceptions import InvalidMessageExceedingSizeException
from stp_core.validators.message_length_validator import MessageLenValidator


MSG_LIMIT = 4
vldtr = MessageLenValidator(MSG_LIMIT)


def test_msg_under_limit():
    vldtr.validate("11".encode())
    pass


def test_msg_over_limit():
    with pytest.raises(InvalidMessageExceedingSizeException):
        vldtr.validate(("1" * (MSG_LIMIT + 1)).encode())
