import pytest
import string
import base58
from plenum.common.messages.fields import Base58Field
from plenum.common.util import randomString
from plenum.test.input_validation.utils import *


LENGTH_LONG_MIN = 43
LENGTH_LONG_MAX = 46
LENGTH_SHORT_MIN = 15
LENGTH_SHORT_MAX = 26

valid_base58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyzzzzaaaaa'
validator = Base58Field(decodedLengthConstraints=(16, 32))


def get_chunk(length):
    chunk = valid_base58[:length]
    return (chunk, len(base58.b58decode(chunk)))

def test_valid_base58():
    for i in range(1, len(valid_base58) + 1):
        (chunk, decoded_len) = get_chunk(i)
        assert not Base58Field().validate(chunk) # no constraints
        assert not Base58Field(
                decodedLengthConstraints=(decoded_len,)).validate(chunk)
        assert Base58Field(
                decodedLengthConstraints=(decoded_len - 1,)).validate(chunk)

def test_empty_string():
    assert not Base58Field().validate('')
    assert Base58Field(
            decodedLengthConstraints=(len(randomString()),)).validate('')


def test_multiple_constraints():
    (chunk1, decoded_len1) = get_chunk(10)
    (chunk2, decoded_len2) = get_chunk(20)
    validator = Base58Field(decodedLengthConstraints=(
        decoded_len1, decoded_len2)
    )
    assert not validator.validate(chunk1) 
    assert not validator.validate(chunk2) 
    assert validator.validate(get_chunk(30)[0]) 


def test_invalid_symbol():
    assert Base58Field().validate(valid_base58[:10] + '0')
