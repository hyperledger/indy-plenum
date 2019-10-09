from plenum.common.messages.node_messages import ViewChangeAck
from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, LimitedLengthStringField

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("viewNo", NonNegativeNumberField),
    ("name", LimitedLengthStringField),
    ("digest", LimitedLengthStringField),
])


def test_hash_expected_type():
    assert ViewChangeAck.typename == "VIEW_CHANGE_ACK"


def test_has_expected_fields():
    actual_field_names = OrderedDict(ViewChangeAck.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ViewChangeAck.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
