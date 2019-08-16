from plenum.common.messages.node_messages import Checkpoint
from collections import OrderedDict
from plenum.common.messages.fields import \
    NonNegativeNumberField, MerkleRootField

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("instId", NonNegativeNumberField),
    ("viewNo", NonNegativeNumberField),
    ("seqNoStart", NonNegativeNumberField),
    ("seqNoEnd", NonNegativeNumberField),
    ("digest", MerkleRootField),
])


def test_hash_expected_type():
    assert Checkpoint.typename == "CHECKPOINT"


def test_has_expected_fields():
    actual_field_names = OrderedDict(Checkpoint.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(Checkpoint.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
