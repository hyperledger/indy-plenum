import pytest
from plenum.common.messages.fields import NonNegativeNumberField

validator = NonNegativeNumberField()


def test_positive_number():
    assert not validator.validate(1)


def test_negative_number():
    assert validator.validate(-1)


def test_zero_number():
    assert not validator.validate(0)


def test_not_accepts_floats():
    assert validator.validate(1.5)
