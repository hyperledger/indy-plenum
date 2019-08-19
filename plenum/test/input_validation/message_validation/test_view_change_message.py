from plenum.common.messages.node_messages import ViewChange
from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, IterableField

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("viewNo", NonNegativeNumberField),
    ("stableCheckpoint", NonNegativeNumberField),
    ("prepared", IterableField),
    ("preprepared", IterableField),
    ("checkpoints", IterableField)
])


def test_hash_expected_type():
    assert ViewChange.typename == "VIEW_CHANGE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(ViewChange.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ViewChange.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
