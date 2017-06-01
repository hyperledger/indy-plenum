import pytest
import string
from plenum.common.messages.fields import Base58Field
from plenum.common.util import randomString
from plenum.test.input_validation.utils import *


LENGTH_LONG_MIN = 43
LENGTH_LONG_MAX = 46
LENGTH_SHORT_MIN = 15
LENGTH_SHORT_MAX = 26

valid_base58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyzzzzaaaaa'
validator = Base58Field(short=True, long=True)


def test_valid_base58():
    assert not validator.validate(valid_base58[:LENGTH_LONG_MIN])
    assert not validator.validate(valid_base58[:LENGTH_SHORT_MIN])


def test_empty_string():
    assert validator.validate('')


def test_wrong_lengths():
    assert validator.validate(valid_base58[:LENGTH_LONG_MIN - 1])
    assert validator.validate(valid_base58[:LENGTH_LONG_MAX + 1])
    assert validator.validate(valid_base58[:LENGTH_SHORT_MIN - 1])
    assert validator.validate(valid_base58[:LENGTH_SHORT_MAX + 1])


def test_invalid_symbol():
    assert validator.validate(valid_base58[:LENGTH_LONG_MIN - 1] + '0')
