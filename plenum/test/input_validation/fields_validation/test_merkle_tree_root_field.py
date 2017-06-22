from plenum.common.messages.fields import MerkleRootField
from plenum.test.input_validation.constants import TEST_B58_BY_DECODED_LEN

validator = MerkleRootField()

def test_non_empty_merkle_roots():
    for decoded_len, val in TEST_B58_BY_DECODED_LEN.items():
        if decoded_len == 32:
            assert not validator.validate(val)
        else:
            assert validator.validate(val)

def test_empty_string():
    assert validator.validate('')

def test_invalid_symbol():
    res = validator.validate(TEST_B58_BY_DECODED_LEN[32][:-1] + '0')
    assert res
    assert res == "should not contains the following chars {}". \
            format(sorted(set('0')))
