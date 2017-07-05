import pytest
from plenum.common.messages.fields import SerializedValueField

validator = SerializedValueField()


def test_non_empty_string():
    assert not validator.validate("x")


def test_empty_string():
    assert validator.validate("")


def test_non_empty_bytes():
    assert not validator.validate(b"hello")


def test_empty_bytes():
    assert validator.validate(b"")
