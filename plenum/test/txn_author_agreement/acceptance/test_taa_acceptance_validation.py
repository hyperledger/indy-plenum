import pytest

from plenum.common.exceptions import InvalidClientTAAAcceptance
from plenum.common.constants import (
    POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID, AUDIT_LEDGER_ID
)


# The author agreement is must have for all Domain transactions
# Plugins must be able to specify for what ledgers the author agreement is also must have
# Enhance dynamic validation as follows:
#    If this is DOMAIN txn, or a Plugin txn from a ledger for which TAA is required - process. Otherwise - OK.
#    Get the latest TAA (using 'last_taa' key in state)
#    If there is no TAA - OK
#    Get the latest AML (using 'last_aml' key in state)
#    If there is no AML - REJECT
#    Get the TAA's hash and compare with the one in the request. If they are not equal - REJECT
#    Get the request's timestamp. Make sure that the ts is in the interval [TAA's ts - 2 mins; current PP time + 2 mins]. If not - REJECT
#    Get the requests' acceptance mechanism string. Make sure that it's present in the latest AML. If not - REJECT

# TODO
# - taa activation per ledger
# - test node configuration regarding taa necessity
# - add flag 'taa_acceptance_required' to LedgerInfo class
#   - ledger_info API in ledger manager + tests


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
    assert node_validator.ledgerManager.ledger_info[ledger_id].taa_acceptance_required == required


@pytest.mark.taa_acceptance_missed
def test_taa_acceptance_missed_during_enabled_taa(node_validator, validate, req, operation):
    ledger_id = node_validator.ledger_id_for_request(req)

    if node_validator.ledgerManager.ledger_info[ledger_id].taa_acceptance_required:
        with pytest.raises(
            InvalidClientTAAAcceptance,
            match=("Txn Author Agreement is required for ledger with id {}"
                   .format(ledger_id))
        ):
            validate(req)
    else:
        validate(req)


@pytest.mark.taa_acceptance_digest('123456')
def test_taa_acceptance_digest_non_latest(validate, domain_req):
    # TODO checks:
    #   - is rejected
    #   - rejection reason contains expected digest
    with pytest.raises(
        InvalidClientTAAAcceptance,
        match=("Accepted Txn Author Agreement is invalid or non-latest, expected {}"
               .format(latest_taa_digest))
    ):
        validate(domain_req)


@pytest.mark.taa_acceptance_mechanism('some-unknown-mech')
def test_taa_acceptance_mechanism_unknown(validate, domain_req):
    with pytest.raises(
        InvalidClientTAAAcceptance,
        match=(
            "Accepted Txn Author Agreement is invalid or non-latest, expected {}"
            .format(latest_taa_digest)
        )
    ):
        validate(domain_req)


@pytest.mark.taa_acceptance_time()
def test_taa_acceptance_time_too_old(validate, domain_req):
    # TODO: ??? requirements
    # - too old
    # - too new
    with pytest.raises(
        InvalidClientTAAAcceptance,
        match=("Acception time of Txn Author Agreement is not appropriate")
    ):
        validate(domain_req)


# TODO test name
def test_taa_acceptance_time_too_recent(validate, domain_req):
    # TODO: ??? requirements
    # - too old
    # - too new
    with pytest.raises(
        InvalidClientTAAAcceptance,
        match=("Acception time of Txn Author Agreement is not appropriate")
    ):
        validate(domain_req)


def test_taa_acceptance_valid(validate, domain_req):
    # TODO valid:
    #   - digest
    #   - time
    #   - mechanism
    validate(domain_req)
