from plenum.common.messages.fields import MerkleRootField
from plenum.test.input_validation.utils import b58_by_len

validator = MerkleRootField()


def test_non_empty_merkle_roots():
    for byte_len in range(1, 33):
        val = b58_by_len(byte_len)
        if byte_len == 32:
            assert not validator.validate(val)
        else:
            assert validator.validate(val)


def test_empty_string():
    assert validator.validate('')


def test_invalid_symbol():
    res = validator.validate(b58_by_len(32)[:-1] + '0')
    assert res
    assert (res == "should not contain the following chars {}".
            format(sorted(set('0'))))
