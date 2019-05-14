import json

import pytest
from indy.ledger import build_txn_author_agreement_request

from plenum.common.constants import (
    TXN_AUTHOR_AGREEMENT_VERSION, TXN_AUTHOR_AGREEMENT_TEXT,
    TXN_PAYLOAD, TXN_METADATA, TXN_METADATA_SEQ_NO, TXN_METADATA_TIME
)
from plenum.common.exceptions import RequestNackedException, RequestRejectedException
from plenum.common.types import OPERATION, f
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.helper import sdk_get_and_check_replies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request
from .helper import (
    get_config_req_handler, sdk_send_txn_author_agreement,
    expected_state_data, TaaData, gen_random_txn_author_agreement
)


def test_send_valid_txn_author_agreement_succeeds(
    looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee, random_taa
):
    version, text  = random_taa
    reply = sdk_send_txn_author_agreement(
        looper, sdk_pool_handle, sdk_wallet_trustee, version, text)[0]
    digest = ConfigReqHandler._taa_digest(version, text).decode()

    data = expected_state_data(TaaData(
        version, text,
        reply[1][f.RESULT.nm][TXN_METADATA][TXN_METADATA_SEQ_NO],
        reply[1][f.RESULT.nm][TXN_METADATA][TXN_METADATA_TIME]
    ))

    # TODO: Replace this with get transaction
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    for node in txnPoolNodeSet:
        config_req_handler = get_config_req_handler(node)
        assert config_req_handler.get_taa_digest() == digest
        assert config_req_handler.get_taa_digest(version) == digest

        assert config_req_handler.get_taa_data() == data
        assert config_req_handler.get_taa_data(version=version) == data
        assert config_req_handler.get_taa_data(digest=digest) == data


def test_send_invalid_txn_author_agreement_fails(
    looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee, random_taa
):
    req = looper.loop.run_until_complete(build_txn_author_agreement_request(sdk_wallet_trustee[1], *random_taa))
    req = json.loads(req)
    req[OPERATION]['text'] = 42
    rep = sdk_sign_and_send_prepared_request(looper, sdk_wallet_trustee, sdk_pool_handle, json.dumps(req))
    with pytest.raises(RequestNackedException):
        sdk_get_and_check_replies(looper, [rep])


def test_send_valid_txn_author_agreement_without_enough_privileges_fails(
    looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, random_taa
):
    with pytest.raises(RequestRejectedException):
        sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_steward,
                                      *random_taa)


def test_send_different_txn_author_agreement_with_same_version_fails(
    looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee
):
    # Send original txn
    version, text = gen_random_txn_author_agreement()
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                  version, text)

    _, text = gen_random_txn_author_agreement()
    # Send new txn with old version
    with pytest.raises(RequestRejectedException):
        sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                      version, text)
