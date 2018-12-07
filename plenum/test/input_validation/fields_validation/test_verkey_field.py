from plenum.common.messages.fields import VerkeyField

from plenum.test.input_validation.utils import b58_by_len

validator = VerkeyField()
err = 'Neither a full verkey nor an abbreviated one'


def test_non_empty_verkeys():
    for byte_len in range(1, 33):
        val = b58_by_len(byte_len)
        as_long = validator.validate(val)
        as_short = validator.validate('~' + val)
        if byte_len == 16:
            assert as_long.startswith(err)
            assert not as_short
        elif byte_len == 32:
            assert not as_long
            assert as_short.startswith(err)
        else:
            assert as_long.startswith(err)
            assert as_short.startswith(err)


def test_empty_verkey():
    res = validator.validate('')
    assert res.startswith(err)