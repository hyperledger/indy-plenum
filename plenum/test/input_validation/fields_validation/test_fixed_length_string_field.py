import pytest
from plenum.common.messages.fields import FixedLengthField


def test_length():
    with pytest.raises(ValueError):
        FixedLengthField(length=-1)

    with pytest.raises(ValueError):
        FixedLengthField(length=0)

    with pytest.raises(TypeError):
        FixedLengthField(length=1.5)

    FixedLengthField(length=1)
    FixedLengthField(length=24)
    FixedLengthField(length=100)


def test_valid_string():
    validator = FixedLengthField(length=10)
    assert not validator.validate("0123456789")
    assert validator.validate("01234567891")
    assert validator.validate("01234567")
