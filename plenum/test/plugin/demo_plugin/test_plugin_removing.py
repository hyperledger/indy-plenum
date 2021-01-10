import pytest

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.restart.helper import restart_nodes
from plenum.test.test_node import ensureElectionsDone

from plenum.test.pool_transactions.helper import sdk_build_get_txn_request, sdk_sign_and_send_prepared_request

from plenum.common.txn_util import get_seq_no
from plenum.test.freshness.helper import get_multi_sig_values_for_all_nodes, \
    check_updated_bls_multi_sig_for_ledger, check_freshness_updated_for_ledger
from plenum.test.helper import freshness, sdk_get_and_check_replies
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.helper import send_auction_txn
from stp_core.loop.eventually import eventually

FRESHNESS_TIMEOUT = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with freshness(tconf, enabled=True, timeout=FRESHNESS_TIMEOUT):
        yield tconf


def check_get_auction_txn(expected_result,
                          looper,
                          sdk_wallet_steward,
                          sdk_pool_handle):
    seqNo = get_seq_no(expected_result)

    _, steward_did = sdk_wallet_steward
    request = sdk_build_get_txn_request(looper, steward_did, seqNo)

    request_couple = \
        sdk_sign_and_send_prepared_request(looper,
                                           sdk_wallet_steward,
                                           sdk_pool_handle,
                                           request)
    result = sdk_get_and_check_replies(looper,
                                        [request_couple])[0][1]['result']

    assert expected_result['reqSignature'] == result['data']['reqSignature']
    assert expected_result['txn'] == result['data']['txn']
    assert expected_result['txnMetadata'] == result['data']['txnMetadata']
    assert expected_result['rootHash'] == result['data']['rootHash']
    assert expected_result['ver'] == result['data']['ver']
    assert expected_result['auditPath'] == result['data']['auditPath']


def test_update_bls_multi_sig_for_auction_ledger_by_timeout(looper, tconf, txn_pool_node_set_post_creation,
                                                            sdk_pool_handle, sdk_wallet_steward):
    """
    Send a transaction from the plugin
    Wait for recording a freshness txn
    Restart the pool without plugins
    Check that reading the plugin transaction is failed
    Check that sending a plugin transaction is failed
    Check that reading the plugin transaction is successful
    Send an ordinary transaction
    Check pool for freshness and consensus
    """
    txnPoolNodeSet = txn_pool_node_set_post_creation

    # 1. Update auction ledger
    result = send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)

    # get txn
    check_get_auction_txn(result[0][1]['result'], looper,
                                           sdk_wallet_steward,
                                           sdk_pool_handle)

    # 2. Wait for the first freshness update
    looper.run(eventually(
        check_freshness_updated_for_ledger, txnPoolNodeSet, AUCTION_LEDGER_ID,
        timeout=3 * FRESHNESS_TIMEOUT)
    )

    # 3. restart pool
    restart_nodes(looper, txnPoolNodeSet, txnPoolNodeSet, tconf, tdir, allPluginsPath, start_one_by_one=False)

    # get txn should failed with "ledger not exists"
    check_get_auction_txn(result, looper,
                                           sdk_wallet_steward,
                                           sdk_pool_handle)


    # should failed with "unknown txn"
    send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)


    # make sure that all node have equal Priamries and can order
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=20)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
