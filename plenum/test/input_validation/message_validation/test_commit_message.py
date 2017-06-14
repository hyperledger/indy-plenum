import pytest
from plenum.common.types import Commit
from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField

EXPECTED_ORDERED_FIELDS = ["instId", "viewNo", "ppSeqNo"]
EXPECTED_ORDERED_VALIDATORS = [NonNegativeNumberField,
                               NonNegativeNumberField,
                               NonNegativeNumberField]


def test_hash_expected_type():
    assert Commit.typename == "COMMIT"


def test_has_expected_fields():
    actual_field_names = list(OrderedDict(Commit.schema).keys())
    assert actual_field_names == EXPECTED_ORDERED_FIELDS


def test_has_expected_validators():
    schema = dict(Commit.schema)
    for field, validator in zip(EXPECTED_ORDERED_FIELDS,
                                EXPECTED_ORDERED_VALIDATORS):
        assert isinstance(schema[field], validator)
