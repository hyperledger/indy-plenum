import pytest

from plenum.common.constants import (
    POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, AUDIT_LEDGER_ID
)


TAA_DISABLED = True


@pytest.mark.parametrize(
    "ledger_id,required",
    [
        (POOL_LEDGER_ID, False),
        (DOMAIN_LEDGER_ID, True),
        (CONFIG_LEDGER_ID, False),
        (AUDIT_LEDGER_ID, False),
    ]
)
def test_ledger_requires_taa_acceptance_default(node_validator, ledger_id, required):
    assert required == node_validator.ledgerManager.ledgerRegistry[ledger_id].taa_acceptance_required


@pytest.mark.taa_acceptance_missed
def test_taa_acceptance_missed_during_disabled_taa(
    node_validator, validate_taa_acceptance, req
):
    validate_taa_acceptance(req)
