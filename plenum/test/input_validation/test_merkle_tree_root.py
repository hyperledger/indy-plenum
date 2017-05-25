import pytest
import string
from plenum.common.messages.fields import MerkleRootField
from plenum.common.util import randomString
from plenum.test.input_validation.utils import *


VALID_HASH_LENGTH = 44
INVALID_HASH_LENGTH = 22


def test_length_validation():
    validator = MerkleRootField()

    valid_hash = randomString(VALID_HASH_LENGTH, validator.alphabet)
    invalid_hash = randomString(INVALID_HASH_LENGTH, validator.alphabet)

    assert_valid(validator.validate(valid_hash))
    assert_invalid(validator.validate(invalid_hash))


def test_alphabet_validation():
    validator = MerkleRootField()

    universal_alphabet = set(string.printable)

    valid_chars = validator.alphabet
    invalid_chars = universal_alphabet - valid_chars

    assert invalid_chars

    valid_hash = randomString(VALID_HASH_LENGTH, valid_chars)
    invalid_hash = randomString(VALID_HASH_LENGTH, invalid_chars)

    assert_valid(validator.validate(valid_hash))
    assert_invalid(validator.validate(invalid_hash))
