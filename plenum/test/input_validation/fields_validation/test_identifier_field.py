import pytest
import base58
from plenum.common.messages.fields import IdentifierField

validator = IdentifierField()

from plenum.test.input_validation.constants import TEST_B58_BY_DECODED_LEN

def test_identifiers():
    for decoded_len, val in TEST_B58_BY_DECODED_LEN.items():
        if decoded_len in (16, 32):
            assert not validator.validate(val)
        else:
            assert validator.validate(val)

def test_invalid_char():
    res = validator.validate(TEST_B58_BY_DECODED_LEN[16][:-1] + '+')
    assert res
    assert res == "should not contains the following chars {}".format(set('+'))
