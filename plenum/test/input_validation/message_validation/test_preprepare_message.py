from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, \
    LedgerIdField, IterableField, NonEmptyStringField, \
    TimestampField, MerkleRootField
from plenum.common.messages.node_messages import PrePrepare

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("instId", NonNegativeNumberField),
    ("viewNo", NonNegativeNumberField),
    ("ppSeqNo", NonNegativeNumberField),
    ("ppTime", TimestampField),
    ("reqIdr", IterableField),
    ("discarded", NonNegativeNumberField),
    ("digest", NonEmptyStringField),
    ("ledgerId", LedgerIdField),
    ("stateRootHash", MerkleRootField),
    ("txnRootHash", MerkleRootField),
])


def test_has_expected_type():
    assert PrePrepare.typename == "PREPREPARE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(PrePrepare.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(PrePrepare.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
