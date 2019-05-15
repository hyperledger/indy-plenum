import pytest
import json

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
    expected_data, TaaData, gen_random_txn_author_agreement,
    calc_taa_digest
)


def test_send_valid_txn_author_agreement_succeeds(
    looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee, random_taa,
    get_txn_author_agreement
):
    reply = sdk_send_txn_author_agreement(
        looper, sdk_pool_handle, sdk_wallet_trustee, *random_taa)[0]
    digest = calc_taa_digest(*random_taa)

    data = TaaData(
        *random_taa,
        seq_no=reply[1][f.RESULT.nm][TXN_METADATA][TXN_METADATA_SEQ_NO],
        txn_time=reply[1][f.RESULT.nm][TXN_METADATA][TXN_METADATA_TIME]
    )

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    for node in txnPoolNodeSet:
        taa_data, taa_digest = get_txn_author_agreement(node=node)
        assert taa_digest == digest
        assert taa_data == data

        taa_data, taa_digest = get_txn_author_agreement(node=node, version=data.version)
        assert taa_digest == digest
        assert taa_data == data

        taa_data, _ = get_txn_author_agreement(node=node, digest=digest)
        assert taa_data == data


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
    text, version = gen_random_txn_author_agreement()
    sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                  text, version)

    text, _ = gen_random_txn_author_agreement()
    # Send new txn with old version
    with pytest.raises(RequestRejectedException):
        sdk_send_txn_author_agreement(looper, sdk_pool_handle, sdk_wallet_trustee,
                                      text, version)
