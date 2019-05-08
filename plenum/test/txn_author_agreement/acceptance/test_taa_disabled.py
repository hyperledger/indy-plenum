import pytest


@pytest.mark.taa_disabled
@pytest.mark.taa_acceptance_missed
def test_taa_acceptance_missed_during_disabled_taa(node_validator, validate, req):
    ledger_id = node_validator.ledger_id_for_request(req)
    assert node_validator.ledgerManager.ledger_info[ledger_id].taa_acceptance_required
    validate(req)
