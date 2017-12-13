import pytest
from plenum.common.messages.fields import LedgerIdField
from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID

validator = LedgerIdField()


def test_valid_ledger_id():
    assert not validator.validate(POOL_LEDGER_ID)
    assert not validator.validate(DOMAIN_LEDGER_ID)


def test_invalid_ledger_id():
    not_existing_ledger = 100
    assert validator.validate(not_existing_ledger)


def test_plugin_ledger_ids():
    orig_ledger_ids = LedgerIdField.ledger_ids
    LedgerIdField.ledger_ids += (100, 200)
    validator = LedgerIdField()
    assert not validator.validate(POOL_LEDGER_ID)
    assert not validator.validate(DOMAIN_LEDGER_ID)
    assert not validator.validate(100)
    assert not validator.validate(200)
    assert validator.validate(300)
    LedgerIdField.ledger_ids = orig_ledger_ids
