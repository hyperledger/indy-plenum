from plenum.common.messages.node_messages import NewView
from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, IterableField, AnyField, LimitedLengthStringField

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("viewNo", NonNegativeNumberField),
    ("viewChanges", IterableField),
    ("checkpoint", AnyField),
    ("batches", IterableField),
    ("primary", LimitedLengthStringField)
])


def test_hash_expected_type():
    assert NewView.typename == "NEW_VIEW"


def test_has_expected_fields():
    actual_field_names = OrderedDict(NewView.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(NewView.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
