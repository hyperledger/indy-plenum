import pytest

from common.exceptions import PlenumTypeError, PlenumValueError
from plenum.common.messages.fields import IterableField, NonNegativeNumberField


def test_invalid_inner_type():
    with pytest.raises(PlenumTypeError):
        IterableField(None)
    with pytest.raises(PlenumTypeError):
        IterableField({})


def test_valid_inner_type():
    validator = IterableField(NonNegativeNumberField())
    assert not validator.validate([1, 2, 3])
    assert validator.validate([1, 2, -3])


def test_valid_min_max_vals():
    with pytest.raises(PlenumValueError):
        IterableField(NonNegativeNumberField(), min_length=0)

    with pytest.raises(PlenumValueError):
        IterableField(NonNegativeNumberField(), max_length=0)

    with pytest.raises(PlenumValueError):
        IterableField(NonNegativeNumberField(), min_length=-1)

    with pytest.raises(PlenumValueError):
        IterableField(NonNegativeNumberField(), max_length=-5)

    with pytest.raises(PlenumTypeError):
        IterableField(NonNegativeNumberField(), min_length=2.5)

    with pytest.raises(PlenumTypeError):
        IterableField(NonNegativeNumberField(), max_length=3.5)

    with pytest.raises(PlenumTypeError):
        IterableField(NonNegativeNumberField(), min_length='2')

    with pytest.raises(PlenumTypeError):
        IterableField(NonNegativeNumberField(), max_length='3')


def test_min_max_checks():
    validator = IterableField(NonNegativeNumberField(), min_length=3,
                              max_length=10)
    assert validator.validate(list(range(2)))

    for i in range(3, 11):
        assert not validator.validate(list(range(i)))

    assert validator.validate(list(range(12)))
