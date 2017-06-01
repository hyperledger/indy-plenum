import pytest
from plenum.common.messages.fields import HexField

valid_hex_hash = "0123456789abcdefABCDEF"
validator = HexField(length=len(valid_hex_hash))


def test_valid_hex():
    assert not validator.validate(valid_hex_hash)


def test_empty_string():
    assert validator.validate('')


def test_invalid_length():
    assert validator.validate(valid_hex_hash[:-1])
    assert validator.validate(valid_hex_hash + "0")


def test_invalid_symbol():
    assert validator.validate(valid_hex_hash[:-1] + 'X')
