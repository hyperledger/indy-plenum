import pytest
import base58
from plenum.common.messages.fields import Base58Field
from plenum.common.util import randomString

from plenum.test.input_validation.utils import b58_by_len


def test_non_empty_base58():
    for byte_len in range(1, 33):
        val = b58_by_len(byte_len)
        assert not Base58Field().validate(val)  # no decoded length constraints
        assert not Base58Field(
            byte_lengths=(byte_len,)).validate(val)
        assert Base58Field(
            byte_lengths=(byte_len - 1,)).validate(val)


def test_empty_string():
    assert not Base58Field().validate('')
    assert Base58Field(
        byte_lengths=(len(randomString()),)).validate('')


def test_multiple_constraints():
    choices = (1, 7, 18)
    validator = Base58Field(byte_lengths=choices)
    for byte_len in range(1, 33):
        val = b58_by_len(byte_len)
        if byte_len in choices:
            assert not validator.validate(val)
        else:
            assert validator.validate(val)


def test_invalid_symbols():
    INVALID_CHARS = '0!@#$%^'
    res = Base58Field().validate(
        b58_by_len(10)[slice(0, -len(INVALID_CHARS))] + INVALID_CHARS)
    assert res
    assert (res == 'should not contain the following chars {}'
            .format(sorted(set(INVALID_CHARS))[:10]))


def test_invalid_symbols_truncated_output():
    INVALID_CHARS = '\x00\xAA0!@#$%^&*()'
    res = Base58Field().validate(
        b58_by_len(20)[slice(0, -len(INVALID_CHARS))] + INVALID_CHARS)
    assert res
    assert (res == 'should not contain the following chars '
            '{} (truncated)'.format(sorted(set(INVALID_CHARS))[:10]))
