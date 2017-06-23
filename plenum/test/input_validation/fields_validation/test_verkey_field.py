import pytest

import base58
from plenum.common.messages.fields import VerkeyField

from plenum.test.input_validation.utils import b58_by_len

validator = VerkeyField()


def test_non_empty_verkeys():
    for byte_len in range(1, 33):
        val = b58_by_len(byte_len)
        as_long = validator.validate(val)
        as_short = validator.validate('~' + val)

        if byte_len == 16:
            assert (as_long ==
                    'b58 decoded value length 16 should be one of [32]')
            assert not as_short
        elif byte_len == 32:
            assert not as_long
            assert (as_short ==
                    'b58 decoded value length 32 should be one of [16]')
        else:
            assert (as_long ==
                    'b58 decoded value length {} should be one of [32]'
                    .format(byte_len))
            assert (as_short ==
                    'b58 decoded value length {} should be one of [16]'
                    .format(byte_len))


def test_empty_verkey():
    res = validator.validate('')
    assert res == 'b58 decoded value length 0 should be one of [32]'
