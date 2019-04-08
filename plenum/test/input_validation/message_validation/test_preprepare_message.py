from collections import OrderedDict

from plenum.common.messages.fields import NonNegativeNumberField, \
    LedgerIdField, IterableField, LimitedLengthStringField, \
    TimestampField, MerkleRootField, BlsMultiSignatureField, AnyMapField, SerializedValueField, BooleanField
from plenum.common.messages.node_messages import PrePrepareMsgData

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("instId", NonNegativeNumberField),
    ("viewNo", NonNegativeNumberField),
    ("ppSeqNo", NonNegativeNumberField),
    ("ppTime", TimestampField),
    ("reqIdr", IterableField),
    ("discarded", SerializedValueField),
    ("digest", LimitedLengthStringField),
    ("ledgerId", LedgerIdField),
    ("stateRootHash", MerkleRootField),
    ("txnRootHash", MerkleRootField),
    ("sub_seq_no", NonNegativeNumberField),
    ("final", BooleanField),
    ("poolStateRootHash", MerkleRootField),
    ("auditTxnRootHash", MerkleRootField),
    ("blsMultiSig", BlsMultiSignatureField),
    ('plugin_fields', AnyMapField)
])


def test_has_expected_type():
    assert PrePrepareMsgData.typename == "PREPREPARE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(PrePrepareMsgData.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(PrePrepareMsgData.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
