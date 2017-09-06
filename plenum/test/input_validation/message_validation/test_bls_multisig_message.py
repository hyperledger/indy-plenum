from collections import OrderedDict

from plenum.common.messages.fields import IterableField, NonEmptyStringField
from plenum.common.messages.node_messages import BlsMultiSignature

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("blsMultiSigNodes", IterableField),
    ("blsMultiSigValue", NonEmptyStringField),
])


def test_has_expected_fields():
    actual_field_names = OrderedDict(BlsMultiSignature.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(BlsMultiSignature.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
