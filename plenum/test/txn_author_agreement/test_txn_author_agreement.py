import pytest
import json

from indy.ledger import build_txn_author_agreement_request

from plenum.common.constants import TXN_AUTHOR_AGREEMENT_VERSION, TXN_AUTHOR_AGREEMENT_TEXT, REPLY
from plenum.common.exceptions import RequestNackedException, RequestRejectedException
from plenum.common.types import OPERATION
from plenum.common.util import randomString
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.helper import sdk_get_and_check_replies, sdk_sign_and_submit_req_obj
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.helper import sdk_get_and_check_replies
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request
from plenum.test.txn_author_agreement.helper import sdk_send_txn_author_agreement, sdk_get_txn_author_agreement


def test_send_taa_before_taa_aml(looper, sdk_pool_handle, sdk_wallet_trustee):
    text = randomString(1024)
    version = randomString(16)
    with pytest.raises(RequestRejectedException) as e:
        sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, text, version)
    assert e.match('TAA txn is forbidden until TAA AML is set. Send TAA AML first')


def test_send_valid_txn_author_agreement_succeeds(looper, setup, txnPoolNodeSet, sdk_pool_handle,
                                                  sdk_wallet_trustee, sdk_wallet_client):
    text = randomString(1024)
    version = randomString(16)
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee, text, version)

    reply = sdk_get_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_client)[1]
    assert reply['op'] == REPLY

    result = reply['result']['data']
    assert result[TXN_AUTHOR_AGREEMENT_TEXT] == text
    assert result[TXN_AUTHOR_AGREEMENT_VERSION] == version


def test_send_invalid_txn_author_agreement_fails(looper, setup, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee):
    req = looper.loop.run_until_complete(build_txn_author_agreement_request(sdk_wallet_trustee[1],
                                                                            randomString(1024), randomString(16)))
    req = json.loads(req)
    req[OPERATION]['text'] = 42
    rep = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, json.dumps(req))
    with pytest.raises(RequestNackedException):
        sdk_get_and_check_replies(looper, [rep])


def test_send_valid_txn_author_agreement_without_enough_privileges_fails(looper, setup, txnPoolNodeSet, sdk_pool_handle,
                                                                         sdk_wallet_steward):
    with pytest.raises(RequestRejectedException):
        sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_steward,
                                      randomString(1024), randomString(16))


def test_send_different_txn_author_agreement_with_same_version_fails(looper, setup, txnPoolNodeSet, sdk_pool_handle,
                                                                     sdk_wallet_trustee):
    # Send original txn
    version = randomString(16)
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                  randomString(1024), version)

    # Send new txn with old version
    with pytest.raises(RequestRejectedException):
        sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                      randomString(1024), version)
