from datetime import datetime, date

import pytest
import json
from random import randint

from indy.ledger import build_txn_author_agreement_request

from plenum.common.util import get_utc_epoch
from plenum.test.delayers import cDelay

from plenum.common.types import f
from plenum.common.constants import AML, DOMAIN_LEDGER_ID
from plenum.test.helper import sdk_sign_and_submit_req
from plenum.test.stasher import delay_rules

from plenum.test.txn_author_agreement.helper import calc_taa_digest, sdk_send_txn_author_agreement_disable, \
    gen_random_txn_author_agreement
from stp_core.loop.eventually import eventually

SEC_PER_DAY = 24 * 60 * 60
TAA_ACCEPTANCE_TIME_BEFORE_AND_AFTER = 3

# make tests stricter
@pytest.fixture(scope="module")
def tconf(tconf):
    old_lower = tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME
    old_upper = tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME
    tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME = TAA_ACCEPTANCE_TIME_BEFORE_AND_AFTER
    tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME = TAA_ACCEPTANCE_TIME_BEFORE_AND_AFTER
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
            "Incorrect Txn Author Agreement"
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


def test_taa_acceptance_with_incorrect_time(
    validate_taa_acceptance, validation_error,
    request_dict
):
    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] *= 1000
    with pytest.raises(
        validation_error,
        match=(
            r"TAA_ACCEPTANCE_TIME = {} is "
            r"out of range.".format(request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm])
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

    lower_threshold = datetime.utcfromtimestamp(taa_ts -
                                                tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME).date()
    upper_threshold = datetime.utcfromtimestamp(pp_time +
                                                tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME).date()
    lower_threshold_ts = int((lower_threshold -
                              date(1970, 1, 1)).total_seconds())

    patch_pp_time(txnPoolNodeSet, monkeypatch, pp_time)

    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = lower_threshold_ts
    validate_taa_acceptance(request_dict)

    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = lower_threshold_ts - SEC_PER_DAY
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
    pp_time = max_last_accepted_pre_prepare_time

    lower_threshold = datetime.utcfromtimestamp(taa_ts -
                                                tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME).date()
    upper_threshold = datetime.utcfromtimestamp(pp_time +
                                                tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME).date()
    upper_threshold_ts = int((upper_threshold -
                              date(1970, 1, 1)).total_seconds())

    patch_pp_time(txnPoolNodeSet, monkeypatch, pp_time)
    patch_now(txnPoolNodeSet, monkeypatch, now=upper_threshold)

    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = upper_threshold_ts
    validate_taa_acceptance(request_dict)

    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = upper_threshold_ts + SEC_PER_DAY
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
        max_last_accepted_pre_prepare_time
    )
    lower_threshold = datetime.utcfromtimestamp(taa_ts -
                                                tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_BEFORE_TAA_TIME).date()
    upper_threshold = datetime.utcfromtimestamp(pp_time +
                                                tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME).date()
    now = pp_time + 5 * tconf.TXN_AUTHOR_AGREEMENT_ACCEPTANCE_TIME_AFTER_PP_TIME

    patch_pp_time(txnPoolNodeSet, monkeypatch, pp_time)
    patch_now(txnPoolNodeSet, monkeypatch, now=now)

    upper_threshold_ts = int((upper_threshold -
                              date(1970, 1, 1)).total_seconds())
    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = upper_threshold_ts
    validate_taa_acceptance(request_dict)

    request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] = upper_threshold_ts + SEC_PER_DAY
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

#
#
# @pytest.mark.parametrize('taa_time', [0,
#                                       - TAA_ACCEPTANCE_TIME_BEFORE_AND_AFTER / 2,
#                                       TAA_ACCEPTANCE_TIME_BEFORE_AND_AFTER / 2,
#                                       TAA_ACCEPTANCE_TIME_BEFORE_AND_AFTER / 2,
#                                       ])
# def test_taa_acceptance_uses_too_precise_time(
#     tconf, txnPoolNodeSet, validate_taa_acceptance, validation_error,
#     turn_off_freshness_state_update, max_last_accepted_pre_prepare_time,
#     request_dict, latest_taa, monkeypatch, taa_time
# ):
#     validate_taa_acceptance(request_dict)
#     request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm] += 1
#     with pytest.raises(
#         validation_error,
#         match=(
#             "Txn Author Agreement acceptance time {} is too precise and is a privacy "
#             "risk.".format(request_dict[f.TAA_ACCEPTANCE.nm][f.TAA_ACCEPTANCE_TIME.nm])
#         )
#     ):
#         validate_taa_acceptance(request_dict)


def test_taa_acceptance_valid(
    validate_taa_acceptance, validation_error, request_dict
):
    validate_taa_acceptance(request_dict)


def test_taa_acceptance_valid_on_uncommitted(
        validate_taa_acceptance_func_api,
        txnPoolNodeSet, looper, sdk_wallet_trustee, sdk_pool_handle,
        add_taa_acceptance
):
    text, version = gen_random_txn_author_agreement()
    old_pp_seq_no = txnPoolNodeSet[0].master_replica.last_ordered_3pc[1]

    with delay_rules([n.nodeIbStasher for n in txnPoolNodeSet], cDelay()):
        req = looper.loop.run_until_complete(build_txn_author_agreement_request(sdk_wallet_trustee[1],
                                                                                text, version,
                                                                                ratification_ts=get_utc_epoch() - 600))
        req = sdk_sign_and_submit_req(sdk_pool_handle, sdk_wallet_trustee, req)

        def check():
            assert old_pp_seq_no + 1 == txnPoolNodeSet[0].master_replica._consensus_data.preprepared[-1].pp_seq_no
        looper.run(eventually(check))
        request_json = add_taa_acceptance(
            taa_text=text,
            taa_version=version,
            taa_a_time=get_utc_epoch() // SEC_PER_DAY * SEC_PER_DAY
        )
        request_dict = dict(**json.loads(request_json))

        validate_taa_acceptance_func_api(request_dict)


def test_taa_acceptance_allowed_when_disabled(
    validate_taa_acceptance,
    validation_error,
    set_txn_author_agreement,
    add_taa_acceptance, looper, sdk_pool_handle, sdk_wallet_trustee
):
    taa_data = set_txn_author_agreement()
    request_json = add_taa_acceptance(
        taa_text=taa_data.text,
        taa_version=taa_data.version,
        taa_a_time=taa_data.txn_time // SEC_PER_DAY * SEC_PER_DAY
    )
    request_dict = dict(**json.loads(request_json))

    validate_taa_acceptance(request_dict)

    # disable TAA acceptance
    sdk_send_txn_author_agreement_disable(looper, sdk_pool_handle, sdk_wallet_trustee)

    # formally valid TAA acceptance
    request_json = add_taa_acceptance(
        taa_text=taa_data.text,
        taa_version=taa_data.version,
        taa_a_time=taa_data.txn_time
    )
    request_dict = dict(**json.loads(request_json))
    request_dict[f.REQ_ID.nm] += 1
    validate_taa_acceptance(request_dict)

    # some invalid TAA acceptance
    request_json = add_taa_acceptance(
        taa_text='any-text',
        taa_version='any-version',
        taa_a_time=taa_data.txn_time
    )
    request_dict = dict(**json.loads(request_json))
    request_dict[f.REQ_ID.nm] += 2
    validate_taa_acceptance(request_dict)


@pytest.mark.skip(reason="Need to fix these fixtures!")
def test_taa_acceptance_retired(
        validate_taa_acceptance, validation_error,
        turn_off_freshness_state_update,
        request_dict, latest_taa,
        looper, sdk_pool_handle, sdk_wallet_trustee, set_txn_author_agreement
):
    # Create new txn author agreement
    set_txn_author_agreement()
    # Retire previous txn author agreement
    taa_data = set_txn_author_agreement(latest_taa.text, latest_taa.version, retired=1)
    # Check that request with original author agreement is discarded
    with pytest.raises(
            validation_error,
            match=("Txn Author Agreement is retired: version {}".format(latest_taa.version))
    ):
        validate_taa_acceptance(request_dict)
    # TODO: Check that transaction is accepted when sent with a new TAA accepted
    # validate_taa_acceptance(request_dict)
