import pytest
from plenum.common.messages.fields import RequestIdentifierField
from datetime import datetime
validator = RequestIdentifierField()

# Request id consists of non-empty string and timestamp
valid_request_id = ("some_string", datetime.now().timestamp())


def test_valid_request_id():
    assert not validator.validate(valid_request_id)


def test_invalid_order():
    s, t = valid_request_id
    assert validator.validate((t, s))


def test_empty_string():
    assert validator.validate(("", valid_request_id[1]))
    assert validator.validate((None, valid_request_id[1]))
