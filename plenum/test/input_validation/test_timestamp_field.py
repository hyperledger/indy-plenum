import pytest
from plenum.common.messages.fields import TimestampField
from plenum.test.input_validation.utils import *


def test_value_range_validation():
    max_possible_timestamp_value = 253402290000.0 - 1  # unix time limitation
    validator = TimestampField()
    assert_valid(validator.validate(max_possible_timestamp_value))
    assert_invalid(validator.validate(max_possible_timestamp_value + 1))
