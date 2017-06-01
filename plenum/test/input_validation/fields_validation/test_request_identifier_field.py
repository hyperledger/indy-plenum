import pytest
from plenum.common.messages.fields import RequestIdentifierField

validator = RequestIdentifierField()

# Request id consists of client identifier (base56 string 16/32 long) and
# some number (for now it is current timestamp, but can be any number)
valid_request_id_long = ("123456789ABCDEFGHJKLMNPQRSTUVWXY", 11111)
valid_request_id_short = ("123456789ABCDEFG", 11111)


def test_valid_request_id():
    assert not validator.validate(valid_request_id_long)
    assert not validator.validate(valid_request_id_short)


def test_invalid_order():
    s, t = valid_request_id_short
    assert validator.validate((t, s))


def test_empty_client_id():
    assert validator.validate(("", valid_request_id_short[1]))
    assert validator.validate((None, valid_request_id_short[1]))


def test_empty_number():
    assert validator.validate((valid_request_id_short[0], None))


def test_invalid_char():
    invalid_client_id = valid_request_id_short[0][:-1] + "0"
    invalid_request = (invalid_client_id, valid_request_id_short[1])
    assert validator.validate(invalid_request)


def test_invalid_length():
    invalid_client_id = valid_request_id_short[0] + "1"
    invalid_request = (invalid_client_id, valid_request_id_short[1])
    assert validator.validate(invalid_request)
