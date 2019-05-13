import json

import pytest

from plenum.common.constants import TXN_AUTHOR_AGREEMENT_VERSION, TXN_AUTHOR_AGREEMENT_TEXT
from plenum.common.exceptions import RequestNackedException, RequestRejectedException
from plenum.common.types import OPERATION
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.helper import sdk_get_and_check_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request
from plenum.test.txn_author_agreement.helper import get_config_req_handler, \
    prepare_txn_author_agreement


def test_send_valid_txn_athr_agrmt_succeeds(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee):
    req = looper.loop.run_until_complete(prepare_txn_author_agreement(sdk_wallet_trustee[1]))
    rep = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, req)
    sdk_get_and_check_replies(looper, [rep])

    req = json.loads(req)
    version = req[OPERATION][TXN_AUTHOR_AGREEMENT_VERSION]
    text = req[OPERATION][TXN_AUTHOR_AGREEMENT_TEXT]
    digest = ConfigReqHandler._taa_digest(version, text)

    # TODO: Replace this with get transaction
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    for node in txnPoolNodeSet:
        config_req_handler = get_config_req_handler(node)
        assert config_req_handler.get_taa_digest() == digest.encode()
        assert config_req_handler.get_taa_digest(version) == digest.encode()

        taa = config_req_handler.state.get(ConfigReqHandler._state_path_taa_digest(digest))
        assert taa is not None

        taa = json.loads(taa.decode())
        assert taa[TXN_AUTHOR_AGREEMENT_VERSION] == version
        assert taa[TXN_AUTHOR_AGREEMENT_TEXT] == text


def test_send_invalid_txn_author_agreement_fails(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee):
    req = looper.loop.run_until_complete(prepare_txn_author_agreement(sdk_wallet_trustee[1]))
    req = json.loads(req)
    req[OPERATION]['text'] = 42
    rep = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, json.dumps(req))
    with pytest.raises(RequestNackedException):
        sdk_get_and_check_replies(looper, [rep])


def test_send_valid_txn_author_agreement_without_enough_privileges_fails(looper, txnPoolNodeSet, sdk_pool_handle,
                                                                         sdk_wallet_steward):
    req = looper.loop.run_until_complete(prepare_txn_author_agreement(sdk_wallet_steward[1]))

    rep = sdk_sign_and_send_prepared_request(looper, sdk_wallet_steward, sdk_pool_handle, req)
    with pytest.raises(RequestRejectedException):
        sdk_get_and_check_replies(looper, [rep])


def test_send_different_txn_author_agreement_with_same_version_fails(looper, txnPoolNodeSet, sdk_pool_handle,
                                                                     sdk_wallet_trustee):
    # Send original txn
    req = looper.loop.run_until_complete(prepare_txn_author_agreement(sdk_wallet_trustee[1]))

    req = json.loads(req)
    old_version = req[OPERATION][TXN_AUTHOR_AGREEMENT_VERSION]
    rep = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, json.dumps(req))
    sdk_get_and_check_replies(looper, [rep])

    # Send new txn with old version
    req = looper.loop.run_until_complete(prepare_txn_author_agreement(sdk_wallet_trustee[1]))
    req = json.loads(req)

    req[OPERATION][TXN_AUTHOR_AGREEMENT_VERSION] = old_version
    rep = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, json.dumps(req))
    with pytest.raises(RequestRejectedException):
        sdk_get_and_check_replies(looper, [rep])
