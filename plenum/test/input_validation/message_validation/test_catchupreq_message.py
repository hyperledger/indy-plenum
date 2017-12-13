import pytest
from plenum.common.messages.node_messages import CatchupReq
from collections import OrderedDict
from plenum.common.messages.fields import \
    NonNegativeNumberField, LedgerIdField


EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("ledgerId", LedgerIdField),
    ("seqNoStart", NonNegativeNumberField),
    ("seqNoEnd", NonNegativeNumberField),
    ("catchupTill", NonNegativeNumberField),
])


def test_hash_expected_type():
    assert CatchupReq.typename == "CATCHUP_REQ"


def test_has_expected_fields():
    actual_field_names = OrderedDict(CatchupReq.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(CatchupReq.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
