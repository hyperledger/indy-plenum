import pytest
from plenum.common.messages.fields import BooleanField

validator = BooleanField()


def test_bool_true():
    assert not validator.validate(True)


def test_bool_false():
    assert not validator.validate(False)


def test_bool_none():
    assert validator.validate(None)


def test_bool_invalid():
    assert validator.validate("True")
