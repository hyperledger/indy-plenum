import pytest
from plenum.common.messages.node_messages import ConsistencyProof
from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, \
    LedgerIdField, MerkleRootField, IterableField

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("ledgerId", LedgerIdField),
    ("seqNoStart", NonNegativeNumberField),
    ("seqNoEnd", NonNegativeNumberField),
    ("viewNo", NonNegativeNumberField),
    ("ppSeqNo", NonNegativeNumberField),
    ("oldMerkleRoot", MerkleRootField),
    ("newMerkleRoot", MerkleRootField),
    ("hashes", IterableField),
])


def test_hash_expected_type():
    assert ConsistencyProof.typename == "CONSISTENCY_PROOF"


def test_has_expected_fields():
    actual_field_names = OrderedDict(ConsistencyProof.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(ConsistencyProof.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
