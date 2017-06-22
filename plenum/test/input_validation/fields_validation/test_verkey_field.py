import pytest
import base58
from plenum.common.messages.fields import VerkeyField

validator = VerkeyField()

from plenum.test.input_validation.constants import TEST_B58_BY_DECODED_LEN

def test_non_empty_verkeys():
    for decoded_len, val in TEST_B58_BY_DECODED_LEN.items():
        as_long = validator.validate(val)
        as_short = validator.validate('~' + val)

        if decoded_len == 16:
            assert as_long == 'b58 decoded value length 16 should be one of [32]'
            assert not as_short
        elif decoded_len == 32:
            assert not as_long
            assert as_short == 'b58 decoded value length 32 should be one of [16]'
        else:
            assert as_long == 'b58 decoded value length {} should be one of [32]'.format(decoded_len)
            assert as_short == 'b58 decoded value length {} should be one of [16]'.format(decoded_len)

def test_empty_verkey():
    res = validator.validate('')
    assert res == 'b58 decoded value length 0 should be one of [32]'
