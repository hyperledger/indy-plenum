import pytest
from plenum.common.messages.fields import NonEmptyStringField

validator = NonEmptyStringField()


def test_non_empty_string():
    assert not validator.validate("x")


def test_empty_string():
    assert validator.validate("")
