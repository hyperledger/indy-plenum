import pytest
from plenum.common.messages.fields import IterableField, NonNegativeNumberField


def test_invalid_inner_type():
    with pytest.raises(Exception):
        IterableField(None)
    with pytest.raises(Exception):
        IterableField({})


def test_valid_inner_type():
    validator = IterableField(NonNegativeNumberField())
    assert not validator.validate([1, 2, 3])
    assert validator.validate([1, 2, -3])
