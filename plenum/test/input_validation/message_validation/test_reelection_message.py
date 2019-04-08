from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, \
    IterableField
from plenum.common.messages.node_messages import ReelectionMsgData

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("instId", NonNegativeNumberField),
    ("round", NonNegativeNumberField),
    ("tieAmong", IterableField),
    ("viewNo", NonNegativeNumberField),
])


def test_hash_expected_type():
    assert ReelectionMsgData.typename == "REELECTION"


def test_has_expected_fields():
    actual_field_names = OrderedDict(ReelectionMsgData.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ReelectionMsgData.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
