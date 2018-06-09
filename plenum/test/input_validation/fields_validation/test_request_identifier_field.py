import pytest

from plenum.common.messages.fields import RequestIdentifierField
from plenum.test.input_validation.constants import \
    TEST_IDENTIFIER_SHORT, TEST_IDENTIFIER_LONG

from plenum.test.input_validation.utils import b58_by_len

validator = RequestIdentifierField()

# Request id consists of client identifier (base56 string 16/32 long) and
# some number (for now it is current timestamp, but can be any number)
valid_request_id = (TEST_IDENTIFIER_LONG, 11111)


def test_valid_request_id():
    for byte_len in range(1, 33):
        val = b58_by_len(byte_len)
        if byte_len in (16, 32):
            assert not validator.validate((val, 11111))
        else:
            assert validator.validate(val)


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
