import pytest

from plenum.common.exceptions import InvalidClientTaaAcceptanceError
from plenum.test.txn_author_agreement.helper import calc_taa_digest
from .conftest import (
    TAA_ACCEPTANCE_TS_TOO_OLD, TAA_ACCEPTANCE_TS_TOO_RECENT
)


@pytest.mark.taa_acceptance_missed
def test_taa_acceptance_missed_during_enabled_taa(
    node_validator, validate_taa_acceptance, req, operation, activate_taa
):
    ledger_id = node_validator.ledger_id_for_request(req)

    if node_validator.ledgerManager.ledgerRegistry[ledger_id].taa_acceptance_required:
        with pytest.raises(
            InvalidClientTaaAcceptanceError,
            match=("Txn Author Agreement acceptance is required for ledger with id {}"
                   .format(ledger_id))
        ):
            validate_taa_acceptance(req)
    else:
        validate_taa_acceptance(req)


@pytest.mark.taa_acceptance_digest(calc_taa_digest('some-taa', 'some-taa-version'))
def test_taa_acceptance_digest_non_latest(
    validate_taa_acceptance, domain_req, activate_taa, latest_taa
):
    # TODO checks:
    #   - is rejected
    #   - rejection reason contains expected digest
    with pytest.raises(
        InvalidClientTaaAcceptanceError,
        match=(
            "Txn Author Agreement acceptance digest is invalid or non-latest:"
            " provided {}, expected {}"
               .format(
                   calc_taa_digest('some-taa', 'some-taa-version'),
                   latest_taa['digest']
                )
        )
    ):
        validate_taa_acceptance(domain_req)


@pytest.mark.skip(reason="INDY-2068")
@pytest.mark.taa_acceptance_mechanism('some-unknown-mech')
def test_taa_acceptance_mechanism_inappropriate(
    validate_taa_acceptance, domain_req, activate_taa, latest_taa
):
    with pytest.raises(
        InvalidClientTaaAcceptanceError,
        match=(
            "Txn Author Agreement acceptance mechanism is inappropriate:"
            " provided {}"
            .format('some-unknown-mech')
        )  # TODO more strict error
    ):
        validate_taa_acceptance(domain_req)


@pytest.mark.taa_acceptance_time(TAA_ACCEPTANCE_TS_TOO_OLD)
def test_taa_acceptance_time_too_old(
    validate_taa_acceptance, domain_req, primary_replica, activate_taa
):
    # TODO: ??? requirements
    # - too old
    # - too new
    with pytest.raises(
        InvalidClientTaaAcceptanceError,
        match=(
            "Txn Author Agreement acceptance time is inappropriate: "
            " provided {}"
            .format(123)
        )
    ):  # TODO more strict error
        validate_taa_acceptance(domain_req)


# TODO test name
@pytest.mark.taa_acceptance_time(TAA_ACCEPTANCE_TS_TOO_RECENT)
def test_taa_acceptance_time_too_recent(
    validate_taa_acceptance, domain_req, activate_taa
):
    # TODO: ??? requirements
    # - too old
    # - too new
    with pytest.raises(
        InvalidClientTaaAcceptanceError,
        match=(
            "Txn Author Agreement acceptance time is inappropriate: "
            " provided {}"
            .format(567)
        )
    ):  # TODO more strict error
        validate_taa_acceptance(domain_req)


def test_taa_acceptance_valid(
    validate_taa_acceptance, domain_req, activate_taa
):
    # TODO valid:
    #   - digest
    #   - time
    #   - mechanism
    validate_taa_acceptance(domain_req)
