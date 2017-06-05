import pytest
from plenum.common.messages.fields import MerkleRootField

LENGTH_MIN = 43
LENGTH_MAX = 45

valid_merkle_root = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
validator = MerkleRootField()


def test_valid_merkle_root():
    assert not validator.validate(valid_merkle_root[:LENGTH_MIN])
    assert not validator.validate(valid_merkle_root[:LENGTH_MAX])


def test_empty_string():
    assert validator.validate('')


def test_wrong_lengths():
    assert validator.validate(valid_merkle_root[:LENGTH_MIN - 1])
    assert validator.validate(valid_merkle_root[:LENGTH_MAX + 1])


def test_invalid_symbol():
    assert validator.validate(valid_merkle_root[:LENGTH_MIN - 1] + '0')
