import pytest
from random import randint

from plenum.common.types import f
from plenum.common.request import Request

from plenum.test.txn_author_agreement.helper import calc_taa_digest


@pytest.fixture(scope='module', autouse=True)
def activate_taa(activate_taa):
    return activate_taa


def patch_pp_time(txnPoolNodeSet, monkeypatch, pp_time):
    for node in txnPoolNodeSet:
        for replica in node.replicas.values():
            monkeypatch.setattr(replica, 'get_time_for_3pc_batch', lambda: pp_time)


def patch_now(txnPoolNodeSet, monkeypatch, now):
    for node in txnPoolNodeSet:
        monkeypatch.setattr(node, 'utc_epoch', lambda *x, **y: now)


@pytest.mark.taa_acceptance_missed
def test_taa_acceptance_missed_during_enabled_taa(
    node_validator, validate_taa_acceptance, validation_error,
    all_request_types, request_dict
):
    ledger_id = node_validator.ledger_id_for_request(
        Request(**request_dict))

    if node_validator.ledgerManager.ledgerRegistry[ledger_id].taa_acceptance_required:
        with pytest.raises(
            validation_error,
            match=("Txn Author Agreement acceptance is required for ledger with id {}"
                   .format(ledger_id))
        ):
            validate_taa_acceptance(request_dict)
    else:
        validate_taa_acceptance(request_dict)


@pytest.mark.taa_accepted(('some-taa', 'some-taa-version'))
def test_taa_acceptance_digest_non_latest(
    validate_taa_acceptance, validation_error, request_dict, latest_taa
):
    taa_digest = request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_DIGEST.nm]
    assert (
        calc_taa_digest('some-taa', 'some-taa-version') ==
        taa_digest
    )

    with pytest.raises(
        validation_error,
        match=(
            "Txn Author Agreement acceptance digest is invalid or non-latest:"
            " provided {}, expected {}"
            .format(
                taa_digest,
                calc_taa_digest(latest_taa.text, latest_taa.version)
            )
        )
    ):
        validate_taa_acceptance(request_dict)


@pytest.mark.taa_acceptance_mechanism('some-unknown-mech')
def test_taa_acceptance_mechanism_inappropriate(
    validate_taa_acceptance, validation_error, request_dict
):
    with pytest.raises(
        validation_error,
        match=(
            "Txn Author Agreement acceptance mechanism is inappropriate:"
            " provided {}"
            .format('some-unknown-mech')
        )  # TODO more strict error
    ):
        validate_taa_acceptance(request_dict)


def test_taa_acceptance_time_near_lower_threshold(
    tconf, txnPoolNodeSet, validate_taa_acceptance, validation_error,
    turn_off_freshness_state_update, max_last_accepted_pre_prepare_time,
    request_dict, latest_taa, monkeypatch
):
    taa_ts = latest_taa.txn_time
    pp_time = max_last_accepted_pre_prepare_time + randint(0, 100)

    lower_threshold = taa_ts - tconf.TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_BEFORE_TAA_TIME
    upper_threshold = pp_time + tconf.TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_AFTER_PP_TIME

    patch_pp_time(txnPoolNodeSet, monkeypatch, pp_time)

    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = lower_threshold
    validate_taa_acceptance(request_dict)

    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = lower_threshold - 1
    with pytest.raises(
        validation_error,
        match=(
            r"Txn Author Agreement acceptance time is inappropriate:"
            " provided {}, expected in \[{}, {}\]"
            .format(
                request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm],
                lower_threshold,
                upper_threshold
            )
        )
    ):
        validate_taa_acceptance(request_dict)


def test_taa_acceptance_time_near_upper_threshold(
    tconf, txnPoolNodeSet, validate_taa_acceptance, validation_error,
    turn_off_freshness_state_update, max_last_accepted_pre_prepare_time,
    request_dict, latest_taa, monkeypatch
):
    taa_ts = latest_taa.txn_time
    pp_time = max_last_accepted_pre_prepare_time + randint(0, 100)

    lower_threshold = taa_ts - tconf.TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_BEFORE_TAA_TIME
    upper_threshold = pp_time + tconf.TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_AFTER_PP_TIME

    patch_pp_time(txnPoolNodeSet, monkeypatch, pp_time)
    patch_now(txnPoolNodeSet, monkeypatch, now=upper_threshold)

    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = upper_threshold
    validate_taa_acceptance(request_dict)

    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = upper_threshold + 1
    with pytest.raises(
        validation_error,
        match=(
            r"Txn Author Agreement acceptance time is inappropriate:"
            " provided {}, expected in \[{}, {}\]"
            .format(
                request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm],
                lower_threshold,
                upper_threshold
            )
        )
    ):
        validate_taa_acceptance(request_dict)


def test_taa_acceptance_valid(
    tconf, validate_taa_acceptance, validation_error, request_dict
):
    validate_taa_acceptance(request_dict)
