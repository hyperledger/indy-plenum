from plenum.common.messages.node_messages import CatchupRep, AnyValueField
from collections import OrderedDict
from plenum.common.messages.fields import \
    IterableField, LedgerIdField, MapField

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("ledgerId", LedgerIdField),
    ("txns", AnyValueField),
    ("consProof", IterableField),
])


def test_hash_expected_type():
    assert CatchupRep.typename == "CATCHUP_REP"


def test_has_expected_fields():
    actual_field_names = OrderedDict(CatchupRep.schema).keys()
    assert list(actual_field_names) == list(EXPECTED_ORDERED_FIELDS.keys())


def test_has_expected_validators():
    schema = dict(CatchupRep.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
