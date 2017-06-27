import pytest
from plenum.common.messages.fields import LimitedLengthStringField


def test_incorrect_max_length():
    with pytest.raises(Exception):
        LimitedLengthStringField(max_length=0)


def test_empty_string():
    validator = LimitedLengthStringField(max_length=1)
    assert validator.validate("")


def test_valid_string():
    validator = LimitedLengthStringField(max_length=1)
    assert not validator.validate("x")


def test_long_string():
    validator = LimitedLengthStringField(max_length=1)
    assert validator.validate("xx")
