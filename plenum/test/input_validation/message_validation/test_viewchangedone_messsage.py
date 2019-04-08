from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, \
    IterableField, LimitedLengthStringField
from plenum.common.messages.node_messages import ViewChangeDoneMsgData

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("viewNo", NonNegativeNumberField),
    ("name", LimitedLengthStringField),
    ("ledgerInfo", IterableField)
])


def test_hash_expected_type():
    assert ViewChangeDoneMsgData.typename == "VIEW_CHANGE_DONE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(ViewChangeDoneMsgData.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ViewChangeDoneMsgData.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
