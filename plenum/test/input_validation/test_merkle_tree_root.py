import pytest
import base58
import string
from plenum.common.messages.fields import MerkleRootField
from plenum.common.util import randomString
from plenum.test.input_validation.utils import *

validLength = 44
invalidLength = 22


def test_length_validation():
    validator = MerkleRootField()
    assert_valid(validator.validate('1' * validLength))
    assert_invalid(validator.validate('1' * invalidLength))


def test_alphabet_validation():
    validator = MerkleRootField()

    universal_alphabet = set(string.printable)

    valid_chars = validator.alphabet
    invalid_chars = universal_alphabet - valid_chars

    assert invalid_chars

    valid_hash = randomString(validLength, ''.join(valid_chars))
    assert_valid(validator.validate(valid_hash))

    invalid_hash = randomString(validLength, ''.join(invalid_chars))
    assert_invalid(validator.validate(invalid_hash))
