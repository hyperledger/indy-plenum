import pytest

from plenum.common.messages.fields import LedgerInfoField

def test_wrong_tuple_size():
    validator = LedgerInfoField()
    assert validator.validate(list(range(0)))
    assert validator.validate(list(range(1)))
    assert validator.validate(list(range(2)))
