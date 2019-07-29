import pytest
import json

from plenum.common.constants import (
    POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, AUDIT_LEDGER_ID
)


@pytest.mark.taa_acceptance_missed
def test_taa_acceptance_missed_when_taa_not_set(
    validate_taa_acceptance, all_request_types, request_dict
):
    validate_taa_acceptance(request_dict)


@pytest.mark.taa_accepted(('any-text', 'any-version'))
def test_taa_acceptance_not_allowed_when_not_expected_for_ledger(
    validate_taa_acceptance, validation_error, pool_request, request_dict
):
    with pytest.raises(
        validation_error,
        match=(
            r"Txn Author Agreement acceptance is not expected"
            " and not allowed in requests for ledger id {}"
            .format(POOL_LEDGER_ID)
        )
    ):
        validate_taa_acceptance(request_dict)


@pytest.mark.taa_accepted(('any-text', 'any-version'))
def test_taa_acceptance_not_allowed_when_not_set_yet(
    validate_taa_acceptance, validation_error, request_dict
):
    with pytest.raises(
        validation_error,
        match=(
            r"Txn Author Agreement acceptance has not been set yet"
            " and not allowed in requests"
        )
    ):
        validate_taa_acceptance(request_dict)
