import pytest

from collections import OrderedDict
from plenum.common.messages.fields import LedgerIdField
from plenum.common.messages.node_messages import ReqLedgerStatus

EXPECTED_ORDERED_FIELDS = OrderedDict([
    ("ledgerId", LedgerIdField),
])


def test_hash_expected_type():
    assert ReqLedgerStatus.typename == "REQ_LEDGER_STATUS"


def test_has_expected_fields():
    actual_field_names = OrderedDict(ReqLedgerStatus.schema).keys()
    assert actual_field_names == EXPECTED_ORDERED_FIELDS.keys()


def test_has_expected_validators():
    schema = dict(ReqLedgerStatus.schema)
    for field, validator in EXPECTED_ORDERED_FIELDS.items():
        assert isinstance(schema[field], validator)
