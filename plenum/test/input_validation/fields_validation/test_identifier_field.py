import pytest
import base58
from plenum.common.messages.fields import IdentifierField

from plenum.test.input_validation.utils import b58_by_len

validator = IdentifierField()


def test_identifiers():
    for byte_len in range(1, 33):
        val = b58_by_len(byte_len)
        if byte_len in (16, 32):
            assert not validator.validate(val)
        else:
            assert validator.validate(val)


def test_invalid_char():
    res = validator.validate(b58_by_len(16)[:-1] + '+')
    assert res
    assert (res == "should not contain the following chars {}".
            format(sorted(set('+'))))
