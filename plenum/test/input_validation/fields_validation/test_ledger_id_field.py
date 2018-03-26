from plenum.common.messages.fields import LedgerIdField
from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID
from plenum import PLUGIN_LEDGER_IDS

validator = LedgerIdField()


def test_valid_ledger_id():
    assert not validator.validate(POOL_LEDGER_ID)
    assert not validator.validate(DOMAIN_LEDGER_ID)


def test_invalid_ledger_id():
    not_existing_ledger = 100
    assert validator.validate(not_existing_ledger)
