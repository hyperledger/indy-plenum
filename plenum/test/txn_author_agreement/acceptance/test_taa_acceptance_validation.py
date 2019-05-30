import pytest
import json
from random import randint

from plenum.common.types import f
from plenum.common.request import Request
from plenum.common.constants import AML, DOMAIN_LEDGER_ID

from plenum.test.txn_author_agreement.helper import calc_taa_digest


# make tests stricter
@pytest.fixture(scope="module")
def tconf(tconf):
    old_lower = tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME
    old_upper = tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME
    tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME = 1
    tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME = 1
    yield tconf
    tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME = old_lower
    tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME = old_upper


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
def test_taa_acceptance_missed_when_taa_set_passed_for_pool_ledger(
    validate_taa_acceptance, pool_request, request_dict
):
    validate_taa_acceptance(request_dict)


@pytest.mark.taa_acceptance_missed
def test_taa_acceptance_missed_when_taa_set_rejected_for_domain_ledger(
    validate_taa_acceptance, validation_error, request_dict
):
    with pytest.raises(
        validation_error,
        match=("Txn Author Agreement acceptance is required for ledger with id {}"
               .format(DOMAIN_LEDGER_ID))
    ):
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
    validate_taa_acceptance, validation_error, request_dict,
    aml_request_kwargs
):
    with pytest.raises(
        validation_error,
        match=(
            r"Txn Author Agreement acceptance mechanism is inappropriate:"
            " provided {}, expected one of {}"
            .format(
                'some-unknown-mech',
                sorted(aml_request_kwargs['operation'][AML])
            ).replace('[', '\[').replace(']', '\]')
        )
    ):
        validate_taa_acceptance(request_dict)


def test_taa_acceptance_time_near_lower_threshold(
    tconf, txnPoolNodeSet, validate_taa_acceptance, validation_error,
    turn_off_freshness_state_update, max_last_accepted_pre_prepare_time,
    request_dict, latest_taa, monkeypatch
):
    taa_ts = latest_taa.txn_time
    pp_time = (
        max_last_accepted_pre_prepare_time +
        randint(0, tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME)
    )

    lower_threshold = taa_ts - tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME
    upper_threshold = pp_time + tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME

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
    pp_time = (
        max_last_accepted_pre_prepare_time +
        randint(0, tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME)
    )

    lower_threshold = taa_ts - tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME
    upper_threshold = pp_time + tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME

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


def test_taa_acceptance_uses_pp_time_instead_of_current_time(
    tconf, txnPoolNodeSet, validate_taa_acceptance, validation_error,
    turn_off_freshness_state_update, max_last_accepted_pre_prepare_time,
    request_dict, latest_taa, monkeypatch
):
    taa_ts = latest_taa.txn_time
    pp_time = (
        max_last_accepted_pre_prepare_time +
        randint(0, tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME)
    )

    lower_threshold = taa_ts - tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME
    upper_threshold = pp_time + tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME
    now = pp_time + 5 * tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME

    patch_pp_time(txnPoolNodeSet, monkeypatch, pp_time)
    patch_now(txnPoolNodeSet, monkeypatch, now=now)

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
    validate_taa_acceptance, validation_error, request_dict
):
    validate_taa_acceptance(request_dict)


def test_taa_acceptance_not_allowed_when_disabled(
    validate_taa_acceptance,
    validation_error,
    set_txn_author_agreement,
    add_taa_acceptance
):
    taa_data = set_txn_author_agreement()
    request_json = add_taa_acceptance(
        taa_text=taa_data.text,
        taa_version=taa_data.version,
        taa_a_time=taa_data.txn_time
    )
    request_dict = dict(**json.loads(request_json))

    validate_taa_acceptance(request_dict)

    # disable TAA acceptance
    taa_data = set_txn_author_agreement("")

    # formally valid TAA acceptance
    request_json = add_taa_acceptance(
        taa_text=taa_data.text,
        taa_version=taa_data.version,
        taa_a_time=taa_data.txn_time
    )
    request_dict = dict(**json.loads(request_json))
    request_dict[f.REQ_ID.nm] += 1
    with pytest.raises(
        validation_error,
        match=(
            r"Txn Author Agreement acceptance is disabled"
            " and not allowed in requests"
        )
    ):
        validate_taa_acceptance(request_dict)

    # some invalid TAA acceptance
    request_json = add_taa_acceptance(
        taa_text='any-text',
        taa_version='any-version',
        taa_a_time=taa_data.txn_time
    )
    request_dict = dict(**json.loads(request_json))
    request_dict[f.REQ_ID.nm] += 2
    with pytest.raises(
        validation_error,
        match=(
            r"Txn Author Agreement acceptance is disabled"
            " and not allowed in requests"
        )
    ):
        validate_taa_acceptance(request_dict)
