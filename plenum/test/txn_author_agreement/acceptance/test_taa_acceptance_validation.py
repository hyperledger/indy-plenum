import pytest

from plenum.common.exceptions import InvalidClientTaaAcceptanceError
from plenum.common.types import f

from plenum.test.txn_author_agreement.helper import calc_taa_digest


@pytest.fixture(scope='module', autouse=True)
def activate_taa(activate_taa):
    return activate_taa


@pytest.mark.taa_acceptance_missed
def test_taa_acceptance_missed_during_enabled_taa(
    node_validator, validate_taa_acceptance, req, operation
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
    validate_taa_acceptance, domain_req, latest_taa
):
    with pytest.raises(
        InvalidClientTaaAcceptanceError,
        match=(
            "Txn Author Agreement acceptance digest is invalid or non-latest:"
            " provided {}, expected {}"
            .format(
                calc_taa_digest('some-taa', 'some-taa-version'),
                calc_taa_digest(latest_taa.text, latest_taa.version)
            )
        )
    ):
        validate_taa_acceptance(domain_req)


@pytest.mark.taa_acceptance_mechanism('some-unknown-mech')
def test_taa_acceptance_mechanism_inappropriate(
    validate_taa_acceptance, domain_req
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


def test_taa_acceptance_time_near_lower_threshold(
    tconf, validate_taa_acceptance, domain_req, latest_taa
):
    taa_ts = latest_taa.txn_time
    pp_time = taa_ts + 1

    lower_threshold = taa_ts - tconf.TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_BEFORE_TAA_TIME
    upper_threshold = pp_time + tconf.TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_AFTER_PP_TIME

    domain_req.taaAcceptance[f.TAA_ACCEPTANCE_TIME.nm] = lower_threshold
    validate_taa_acceptance(domain_req, pp_time)

    domain_req.taaAcceptance[f.TAA_ACCEPTANCE_TIME.nm] = lower_threshold - 1
    with pytest.raises(
        InvalidClientTaaAcceptanceError,
        match=(
            r"Txn Author Agreement acceptance time is inappropriate:"
            " provided {}, expected in \[{}, {}\]"
            .format(
                domain_req.taaAcceptance[f.TAA_ACCEPTANCE_TIME.nm],
                lower_threshold,
                upper_threshold
            )
        )
    ):
        validate_taa_acceptance(domain_req, pp_time)


def test_taa_acceptance_time_near_upper_threshold(
    tconf, validate_taa_acceptance, domain_req, latest_taa
):
    taa_ts = latest_taa.txn_time
    pp_time = taa_ts + 1

    lower_threshold = taa_ts - tconf.TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_BEFORE_TAA_TIME
    upper_threshold = pp_time + tconf.TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_AFTER_PP_TIME

    domain_req.taaAcceptance[f.TAA_ACCEPTANCE_TIME.nm] = upper_threshold
    validate_taa_acceptance(domain_req, pp_time)

    domain_req.taaAcceptance[f.TAA_ACCEPTANCE_TIME.nm] = upper_threshold + 1
    with pytest.raises(
        InvalidClientTaaAcceptanceError,
        match=(
            r"Txn Author Agreement acceptance time is inappropriate:"
            " provided {}, expected in \[{}, {}\]"
            .format(
                domain_req.taaAcceptance[f.TAA_ACCEPTANCE_TIME.nm],
                lower_threshold,
                upper_threshold
            )
        )
    ):  # TODO more strict error
        validate_taa_acceptance(domain_req, pp_time)


def test_taa_acceptance_valid(
    tconf, validate_taa_acceptance, domain_req
):
    pp_time = domain_req.taaAcceptance[f.TAA_ACCEPTANCE_TIME.nm] - tconf.TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_AFTER_PP_TIME + 1
    validate_taa_acceptance(domain_req, pp_time)
