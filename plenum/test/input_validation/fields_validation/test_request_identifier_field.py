import pytest
from plenum.common.messages.fields import RequestIdentifierField

validator = RequestIdentifierField()

valid_client_id_chars = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

MIN_LENGTH_SHORT = 15
MAX_LENGTH_SHORT = 25
MIN_LENGTH_LONG = 43
MAX_LENGTH_LONG = 45

# Request id consists of client identifier (base56 string 16/32 long) and
# some number (for now it is current timestamp, but can be any number)
valid_request_id = (valid_client_id_chars[:MIN_LENGTH_SHORT], 11111)


def test_valid_request_id():
    all_valid_length = \
        list(range(MIN_LENGTH_SHORT, MAX_LENGTH_SHORT + 1)) + \
        list(range(MIN_LENGTH_LONG, MAX_LENGTH_LONG + 1))
    for length in all_valid_length:
        assert not validator.validate((valid_client_id_chars[:length], 11111))


def test_invalid_order():
    s, t = valid_request_id
    assert validator.validate((t, s))


def test_empty_client_id():
    assert validator.validate(("", valid_request_id[1]))
    assert validator.validate((None, valid_request_id[1]))


def test_empty_number():
    assert validator.validate((valid_request_id[0], None))


def test_invalid_char():
    invalid_client_id = valid_request_id[0][:-1] + "0"
    invalid_request = (invalid_client_id, valid_request_id[1])
    assert validator.validate(invalid_request)


def test_invalid_length():
    invalid_client_id = valid_request_id[:-1]
    invalid_request = (invalid_client_id, valid_request_id[1])
    assert validator.validate(invalid_request)
