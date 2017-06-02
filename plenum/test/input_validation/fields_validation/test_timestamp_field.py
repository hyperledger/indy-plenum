import pytest
from plenum.common.messages.fields import TimestampField
from datetime import datetime

validator = TimestampField()
timestamp = datetime.now().timestamp()


def test_valid_value():
    assert not validator.validate(timestamp)

    # This is needed because timestamp is usually multiplied
    # by 1000 to "make it compatible to JavaScript Date()"
    assert not validator.validate(round(timestamp * 1000))


def test_invalid_value():
    assert validator.validate(-1)


