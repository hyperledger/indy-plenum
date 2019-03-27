from collections import OrderedDict
from plenum.common.messages.fields import NonNegativeNumberField, \
    LimitedLengthStringField, MerkleRootField, TimestampField, AnyMapField
from plenum.common.messages.node_messages import Prepare

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("instId", NonNegativeNumberField),
    ("viewNo", NonNegativeNumberField),
    ("ppSeqNo", NonNegativeNumberField),
    ("ppTime", TimestampField),
    ("digest", LimitedLengthStringField),
    ("stateRootHash", MerkleRootField),
    ("txnRootHash", MerkleRootField),
    ("auditTxnRootHash", MerkleRootField),
    ('plugin_fields', AnyMapField)
])


def test_has_expected_type():
    assert Prepare.typename == "PREPARE"


def test_has_expected_fields():
    actual_field_names = OrderedDict(Prepare.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(Prepare.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
