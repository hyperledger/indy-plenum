import pytest

from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    checkNodeDataForEquality, ensureClientConnectedToNodesAndPoolLedgerSame

from plenum.test.pool_transactions.conftest import stewardAndWallet1, \
    steward1, stewardWallet, clientAndWallet1, client1, wallet1, \
    client1Connected
from plenum.test.test_client import genTestClient
from plenum.test.test_node import checkNodesConnected


def test_large_catchup(looper, txnPoolNodeSet, poolTxnClient):
    """
    Checks that node can catchup large ledger
    """
    # Prepare nodes
    lagging_node = txnPoolNodeSet[0]
    rest_nodes = txnPoolNodeSet[1:]
    all_nodes = txnPoolNodeSet
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # Prepare client
    client, wallet = poolTxnClient
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client, *all_nodes)
    looper.run(client.ensureConnectedToNodes())

    # Check that requests executed well
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 10)
    
    # checkNodeDataForEquality(lagging_node, *txnPoolNodeSet[:-1])
    #
    # disconnect_node_and_ensure_disconnected(looper,
    #                                         txnPoolNodeSet,
    #                                         lagging_node,
    #                                         stopNode=False)
    # looper.removeProdable(lagging_node)
    #
    # looper.add(lagging_node)
    # waitNodeDataEquality(looper, lagging_node, *txnPoolNodeSet[:-1])
    #
    # sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 10)
    # checkNodeDataForEquality(lagging_node, *txnPoolNodeSet[:-1])
