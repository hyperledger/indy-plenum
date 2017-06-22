import pytest
import base58
from plenum.common.messages.fields import Base58Field
from plenum.common.util import randomString

from plenum.test.input_validation.constants import TEST_B58_BY_DECODED_LEN


def test_non_empty_base58():
    for decoded_len, val  in TEST_B58_BY_DECODED_LEN.items():
        assert not Base58Field().validate(val) # no decoded length constraints
        assert not Base58Field(
                byte_lengths=(decoded_len,)).validate(val)
        assert Base58Field(
                byte_lengths=(decoded_len - 1,)).validate(val)

def test_empty_string():
    assert not Base58Field().validate('')
    assert Base58Field(
            byte_lengths=(len(randomString()),)).validate('')


def test_multiple_constraints():
    choices = (1, 7, 18)
    validator = Base58Field(byte_lengths=choices)
    for decoded_len, val in TEST_B58_BY_DECODED_LEN.items():
        if decoded_len in choices:
            assert not validator.validate(val)
        else:
            assert validator.validate(val)

def test_invalid_symbol():
    assert Base58Field().validate(TEST_B58_BY_DECODED_LEN[15][:-1] + '0')
