import pytest
from plenum.common.messages.fields import RequestIdentifierField
from datetime import datetime
validator = RequestIdentifierField()

# Request id consists of client identifier and
# some number (for now it is current timestamp, but can be any number)

valid_request_id = ("11111111111111111111111111111111", 11111)


def test_valid_request_id():
    assert not validator.validate(valid_request_id)


def test_invalid_order():
    s, t = valid_request_id
    assert validator.validate((t, s))


def test_empty_client_id():
    assert validator.validate(("", valid_request_id[1]))
    assert validator.validate((None, valid_request_id[1]))


def test_empty_number():
    assert validator.validate((valid_request_id[0], None))
