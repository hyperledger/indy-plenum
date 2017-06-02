import pytest
from plenum.common.messages.fields import IdentifierField

validator = IdentifierField()

valid_chars = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

MIN_LENGTH_SHORT = 15
MAX_LENGTH_SHORT = 25
MIN_LENGTH_LONG = 43
MAX_LENGTH_LONG = 45


def test_valid_identifiers():
    all_valid_length = \
        list(range(MIN_LENGTH_SHORT, MAX_LENGTH_SHORT + 1)) + \
        list(range(MIN_LENGTH_LONG, MAX_LENGTH_LONG + 1))
    for length in all_valid_length:
        assert not validator.validate(valid_chars[:length])


def test_invalid_char():
    invalid_identifier = valid_chars[:MIN_LENGTH_SHORT - 1] + "0"
    assert validator.validate(invalid_identifier)


def test_invalid_length():
    invalid_identifier = valid_chars[:MIN_LENGTH_SHORT - 1]
    assert validator.validate(invalid_identifier)
