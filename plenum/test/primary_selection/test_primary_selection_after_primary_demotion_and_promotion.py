from plenum.common.constants import ALIAS, SERVICES, VALIDATOR
from plenum.test.pool_transactions.conftest import looper
from plenum.test.pool_transactions.helper import updateNodeData

from plenum.test.test_node import ensureElectionsDone
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies

from plenum.test.primary_selection.helper import getPrimaryNodesIdxs


def test_primary_selection_after_demoted_primary_node_promotion(
        looper, txnPoolNodeSet, stewardAndWalletForMasterNode,
        txnPoolMasterNodes):
    """
    Demote primary of master instance, wait for view change and promote it back.
    Check primaries for instances.
    """
    assert len(txnPoolNodeSet) == 4

    # Check primaries after test setup.
    primariesIdxs = getPrimaryNodesIdxs(txnPoolNodeSet)
    assert len(primariesIdxs) == 2
    assert primariesIdxs[0] == 0
    assert primariesIdxs[1] == 1

    master_node = txnPoolMasterNodes[0]
    client, wallet = stewardAndWalletForMasterNode

    # Demote primary of master instance.
    node_data = {
        ALIAS: master_node.name,
        SERVICES: []
    }
    updateNodeData(looper, client, wallet, master_node, node_data)

    restNodes = [node for node in txnPoolNodeSet if node.name != master_node.name]
    ensureElectionsDone(looper, restNodes)

    # Check that there is only one instance now, check it's primary.
    primariesIdxs = getPrimaryNodesIdxs(restNodes)
    assert len(primariesIdxs) == 1
    assert primariesIdxs[0] == 1

    # Ensure pool is working properly.
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, numReqs=3)

    # Promote demoted node back.
    node_data = {
        ALIAS: master_node.name,
        SERVICES: [VALIDATOR]
    }
    updateNodeData(looper, client, wallet, master_node, node_data)

    # Ensure pool is working properly.
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, numReqs=3)

    # Check that there are two instances again, check their primaries.
    primariesIdxs = getPrimaryNodesIdxs(txnPoolNodeSet)
    assert len(primariesIdxs) == 2
    assert primariesIdxs[0] == 2
    assert primariesIdxs[1] == 3
