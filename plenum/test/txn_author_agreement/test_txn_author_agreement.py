import pytest
import json

from indy.ledger import build_txn_author_agreement_request

from plenum.common.exceptions import RequestNackedException, RequestRejectedException
from plenum.common.types import OPERATION, f

from plenum.test.helper import sdk_get_and_check_replies
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request
from .helper import (
    sdk_send_txn_author_agreement,
    gen_random_txn_author_agreement
)


def test_send_valid_txn_author_agreement_before_aml_fails(set_txn_author_agreement):
    with pytest.raises(
            RequestRejectedException,
            match='TAA txn is forbidden until TAA AML is set. Send TAA AML first'
    ):
        set_txn_author_agreement()


def test_send_valid_txn_author_agreement_succeeds(
        set_txn_author_agreement_aml, set_txn_author_agreement, get_txn_author_agreement
):
    # TODO it might make sense to check that update_txn_author_agreement
    # was called with expected set of arguments
    assert set_txn_author_agreement() == get_txn_author_agreement()


def test_send_empty_txn_author_agreement_succeeds(
    set_txn_author_agreement_aml, set_txn_author_agreement, get_txn_author_agreement
):
    assert set_txn_author_agreement(text="") == get_txn_author_agreement()


def test_send_invalid_txn_author_agreement_fails(
        looper, set_txn_author_agreement_aml, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee, random_taa
):
    req = looper.loop.run_until_complete(
        build_txn_author_agreement_request(sdk_wallet_trustee[1], *random_taa)
    )
    req = json.loads(req)
    req[OPERATION]['text'] = 42
    rep = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, json.dumps(req))
    with pytest.raises(RequestNackedException):
        sdk_get_and_check_replies(looper, [rep])


def test_send_valid_txn_author_agreement_without_enough_privileges_fails(
        looper, set_txn_author_agreement_aml, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_steward, random_taa
):
    with pytest.raises(RequestRejectedException):
        sdk_send_txn_author_agreement(
            looper, sdk_pool_handle, sdk_wallet_steward, *random_taa
        )


def test_send_different_txn_author_agreement_with_same_version_fails(
        looper, set_txn_author_agreement_aml, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee
):
    # Send original txn
    text, version = gen_random_txn_author_agreement()
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                  text, version)

    text, _ = gen_random_txn_author_agreement()
    # Send new txn with old version
    with pytest.raises(RequestRejectedException):
        sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                      text, version)
