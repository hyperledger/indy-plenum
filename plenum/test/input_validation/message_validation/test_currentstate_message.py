import pytest

from collections import OrderedDict
from plenum.common.messages.fields import SerializedValueField, NonNegativeNumberField
from plenum.common.messages.node_messages import CurrentState

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("primary", SerializedValueField),
    ("viewNo", NonNegativeNumberField),
])


def test_hash_expected_type():
    assert CurrentState.typename == "CURRENT_STATE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(CurrentState.schema).keys()
    assert actual_field_names == EXPECTED_ORDERED_FIELDS.keys()


def test_has_expected_validators():
    schema = dict(CurrentState.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
