import pytest

from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, \
    NonEmptyStringField
from plenum.common.types import Primary

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("name", NonEmptyStringField),
    ("instId", NonNegativeNumberField),
    ("viewNo", NonNegativeNumberField),
    ("ordSeqNo", NonNegativeNumberField),
])


def test_hash_expected_type():
    assert Primary.typename == "PRIMARY"


def test_has_expected_fields():
    actual_field_names = OrderedDict(Primary.schema).keys()
    assert actual_field_names == EXPECTED_ORDERED_FIELDS.keys()


def test_has_expected_validators():
    schema = dict(Primary.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
