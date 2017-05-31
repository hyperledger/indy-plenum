import pytest
from plenum.common.messages.fields import TimestampField
from plenum.test.input_validation.utils import *
from datetime import datetime


def test_value_range_validation():
    validator = TimestampField()
    timestamp = datetime.now().timestamp()

    assert_valid(validator.validate(timestamp))

    # This is needed because timestamp is usually multiplied
    # by 1000 to "make it compatible to JavaScript Date()"
    assert_valid(validator.validate(round(timestamp * 1000)))

    assert_invalid(validator.validate(-1))


