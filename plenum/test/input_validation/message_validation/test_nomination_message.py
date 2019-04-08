import pytest

from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, \
    LimitedLengthStringField
from plenum.common.messages.node_messages import NominationMsgData

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("name", LimitedLengthStringField),
    ("instId", NonNegativeNumberField),
    ("viewNo", NonNegativeNumberField),
    ("ordSeqNo", NonNegativeNumberField),
])


def test_hash_expected_type():
    assert NominationMsgData.typename == "NOMINATE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(NominationMsgData.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(NominationMsgData.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
