import pytest

from plenum.common.constants import TXN_TYPE, DATA
from plenum.test.node_catchup.helper import waitNodeDataInequality, \
    waitNodeDataEquality
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID

from plenum.test.node_catchup.conftest import sdk_node_created_after_some_txns, \
    sdk_node_set_with_node_added_after_some_txns, sdk_new_node_caught_up
from plenum.test.plugin.demo_plugin.constants import AUCTION_END, PLACE_BID, \
    AMOUNT, AUCTION_START
from plenum.test.plugin.demo_plugin.test_plugin_request_handling import \
    some_requests, successful_op
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, reconnect_node_and_ensure_connected


def test_new_node_catchup_plugin_ledger(txn_pool_node_set_post_creation, looper, some_requests,
                                        sdk_new_node_caught_up):
    """
    A new node catches up the demo plugin's ledger too
    """
    assert len(sdk_new_node_caught_up.getLedger(AUCTION_LEDGER_ID)) > 0
    for node in txn_pool_node_set_post_creation[:-1]:
        assert len(sdk_new_node_caught_up.getLedger(AUCTION_LEDGER_ID)) == \
               len(node.getLedger(AUCTION_LEDGER_ID))


def some_demo_txns(looper, sdk_wallet_steward, sdk_pool_handle):
    id = 'xyz'
    ops = [
        {
            TXN_TYPE: AUCTION_START,
            DATA: {'id': id}
        },
        {
            TXN_TYPE: PLACE_BID,
            DATA: {'id': id, AMOUNT: 20}
        },
        {
            TXN_TYPE: PLACE_BID,
            DATA: {'id': id, AMOUNT: 40}
        },
        {
            TXN_TYPE: AUCTION_END,
            DATA: {'id': id}
        }
    ]

    for op in ops:
        successful_op(looper, op, sdk_wallet_steward, sdk_pool_handle)


@pytest.mark.skip(reason="INDY-1297. Node does not catch up on reconnection anymore.")
def test_disconnected_node_catchup_plugin_ledger_txns(looper,
                                                      txnPoolNodeSet,
                                                      sdk_wallet_client,
                                                      sdk_pool_handle,
                                                      sdk_new_node_caught_up):
    """
    A node gets disconnected, a few config ledger txns happen,
    the disconnected node comes back up and catches up the config ledger
    """
    new_node = sdk_new_node_caught_up
    disconnect_node_and_ensure_disconnected(
        looper, txnPoolNodeSet, new_node, stopNode=False)

    # Do some demo txns;
    some_demo_txns(looper, sdk_wallet_client, sdk_pool_handle)

    # Make sure new node got out of sync
    waitNodeDataInequality(looper, new_node, *txnPoolNodeSet[:-1])

    reconnect_node_and_ensure_connected(looper, txnPoolNodeSet, new_node)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
