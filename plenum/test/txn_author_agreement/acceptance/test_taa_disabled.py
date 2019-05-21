import pytest
import json

from plenum.common.constants import (
    POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, AUDIT_LEDGER_ID
)


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
    validate_taa_acceptance, all_request_types, request_dict
):
    validate_taa_acceptance(request_dict)


# TODO all ledgers ?
def test_taa_acceptance_not_allowed_when_not_set_yet(
    validate_taa_acceptance, validation_error, add_taa_acceptance,
):
    # some invalid TAA acceptance
    request_json = add_taa_acceptance(
        taa_text='any-text', taa_version='any-version')
    request_dict = dict(**json.loads(request_json))
    with pytest.raises(
        validation_error,
        match=(
            r"Txn Author Agreement acceptance has not been set yet"
            " and not allowed in requests"
        )
    ):
        validate_taa_acceptance(request_dict)
