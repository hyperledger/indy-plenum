import pytest

from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField
from plenum.common.messages.node_messages import InstanceChangeMsgData

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("viewNo", NonNegativeNumberField),
    ("reason", NonNegativeNumberField)
])


def test_hash_expected_type():
    assert InstanceChangeMsgData.typename == "INSTANCE_CHANGE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(InstanceChangeMsgData.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(InstanceChangeMsgData.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
