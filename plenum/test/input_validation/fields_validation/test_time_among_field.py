import pytest
from plenum.common.messages.fields import TieAmongField

validator = TieAmongField(max_length=len("Node1:0"))


def test_valid():
    assert not validator.validate(("Node1:0", 1))
    assert not validator.validate(("Node1:0", 0))


def test_invalid_vote_number():
    assert validator.validate(("Node1:0", -1))


def test_empty_node_id():
    assert validator.validate(("", 1))


def test_long_id():
    assert validator.validate(("NNooddee11::00", 1))
