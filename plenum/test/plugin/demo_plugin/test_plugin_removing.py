import pytest

from plenum.common.constants import DATA
from plenum.common.exceptions import RequestNackedException, RequestRejectedException
from plenum.test.freeze_ledgers.helper import sdk_send_freeze_ledgers

from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.plugin.demo_plugin.constants import GET_AUCTION, AUCTION_START
from plenum.test.test_node import ensureElectionsDone

from plenum.test.pool_transactions.helper import sdk_build_get_txn_request, sdk_sign_and_send_prepared_request

from plenum.common.txn_util import get_seq_no, get_payload_data
from plenum.test.freshness.helper import check_freshness_updated_for_ledger
from plenum.test.helper import freshness, sdk_get_and_check_replies, sdk_send_random_and_check
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID
from plenum.test.plugin.demo_plugin.helper import send_auction_txn, send_get_auction_txn, restart_nodes
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
    request = sdk_build_get_txn_request(looper, steward_did, seqNo, ledger_type=str(AUCTION_LEDGER_ID))

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


def test_plugin_removing(looper, tconf, txn_pool_node_set_post_creation,
                         sdk_pool_handle, sdk_wallet_steward, sdk_wallet_trustee, tdir, allPluginsPath):
    """
    Send a transaction from the plugin
    Wait for recording a freshness txn
    Check that reading the plugin transaction is successful
    Check that sending a write plugin transaction is successful
    Freeze the auction ledger
    Check that reading the plugin transaction is successful
    Check that sending a write plugin transaction is failed
    Restart the pool without plugins
    Check that reading the plugin transaction is failed
    Check that sending a write plugin transaction is failed
    Send an ordinary transaction
    Check pool for freshness and consensus
    """
    txnPoolNodeSet = txn_pool_node_set_post_creation

    # Update auction ledger
    result = send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)[0][1]["result"]

    # get txn
    auction_id, auction_name = list(get_payload_data(result)[DATA].items())[0]
    get_auction_result = send_get_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)
    assert get_auction_result[0][1]["result"][auction_id] == auction_name

    # Wait for the first freshness update
    looper.run(eventually(
        check_freshness_updated_for_ledger, txnPoolNodeSet, AUCTION_LEDGER_ID,
        timeout=3 * FRESHNESS_TIMEOUT)
    )

    sdk_send_freeze_ledgers(
        looper, sdk_pool_handle,
        [sdk_wallet_trustee],
        [AUCTION_LEDGER_ID]
    )

    with pytest.raises(RequestRejectedException,
                       match="'{}' transaction is forbidden because of "
                             "'{}' ledger is frozen".format(AUCTION_START, AUCTION_LEDGER_ID)):
        send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)

    # should failed with "ledger is frozen"
    get_auction_result = send_get_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)
    assert get_auction_result[0][1]["result"][auction_id] == auction_name

    # restart pool
    restart_nodes(looper, txnPoolNodeSet, txnPoolNodeSet, tconf, tdir, allPluginsPath, start_one_by_one=True)

    # get txn should failed with "ledger not exists"
    with pytest.raises(RequestNackedException,
                       match="unknown value '" + str(AUCTION_LEDGER_ID)):
        check_get_auction_txn(result, looper,
                              sdk_wallet_steward,
                              sdk_pool_handle)

    # should failed with "unknown txn"
    with pytest.raises(RequestNackedException,
                       match="invalid type: " + GET_AUCTION):
        send_get_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)

    with pytest.raises(RequestNackedException,
                       match="invalid type: " + AUCTION_START):
        send_auction_txn(looper, sdk_pool_handle, sdk_wallet_steward)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, 1)

    # make sure that all node have equal primaries and can order
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet, custom_timeout=20)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
