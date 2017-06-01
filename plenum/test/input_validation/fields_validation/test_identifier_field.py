import pytest
from plenum.common.messages.fields import IdentifierField
from datetime import datetime

validator = IdentifierField()

# Request id consists of client identifier (base56 string 16/32 long) and
# some number (for now it is current timestamp, but can be any number)
valid_identifier_long = "123456789ABCDEFGHJKLMNPQRSTUVWXY"
valid_identifier_short = "123456789ABCDEFG"


def test_valid_identifiers():
    assert not validator.validate(valid_identifier_long)
    assert not validator.validate(valid_identifier_short)


def test_invalid_char():
    invalid_identifier = valid_identifier_short[:-1] + "0"
    assert validator.validate(invalid_identifier)


def test_invalid_length():
    invalid_identifier = valid_identifier_short + "1"
    assert validator.validate(invalid_identifier)
