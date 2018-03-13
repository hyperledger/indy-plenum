from plenum.common.messages.node_messages import ThreePCState
from collections import OrderedDict
from plenum.common.messages.fields import \
    NonNegativeNumberField, IterableField

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("instId", NonNegativeNumberField),
    ("messages", IterableField),
])


def test_hash_expected_type():
    assert ThreePCState.typename == "THREE_PC_STATE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(ThreePCState.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ThreePCState.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
