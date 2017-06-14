import pytest
from plenum.common.types import LedgerStatus
from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, \
    LedgerIdField, MerkleRootField, IterableField

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("ledgerId", LedgerIdField),
    ("txnSeqNo", NonNegativeNumberField),
    ("merkleRoot", MerkleRootField),
])


def test_hash_expected_type():
    assert LedgerStatus.typename == "LEDGER_STATUS"


def test_has_expected_fields():
    actual_field_names = OrderedDict(LedgerStatus.schema).keys()
    assert actual_field_names == EXPECTED_ORDERED_FIELDS.keys()


def test_has_expected_validators():
    schema = dict(LedgerStatus.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
