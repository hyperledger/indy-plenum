import pytest

from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, \
    LedgerIdField, IterableField, TimestampField, HexField, MerkleRootField, \
    AnyValueField, AnyMapField, SerializedValueField
from plenum.common.messages.node_messages import OrderedMsgData

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("instId", NonNegativeNumberField),
    ("viewNo", NonNegativeNumberField),
    ("valid_reqIdr", IterableField),
    ("invalid_reqIdr", IterableField),
    ("ppSeqNo", NonNegativeNumberField),
    ("ppTime", TimestampField),
    ("ledgerId", LedgerIdField),
    ("stateRootHash", MerkleRootField),
    ("txnRootHash", MerkleRootField),
    ("auditTxnRootHash", MerkleRootField),
    ("primaries", IterableField),
    ('plugin_fields', AnyMapField)
])


def test_hash_expected_type():
    assert OrderedMsgData.typename == "ORDERED"


def test_has_expected_fields():
    actual_field_names = OrderedDict(OrderedMsgData.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(OrderedMsgData.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
