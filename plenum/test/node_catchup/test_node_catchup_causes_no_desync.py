from plenum.common.util import compare_3PC_keys
from plenum.test.delayers import pDelay, cDelay, ppDelay
from plenum.test.node_catchup.test_node_reject_invalid_txn_during_catchup import \
    get_any_non_primary_node
from stp_core.common.log import getlogger
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import \
    waitNodeDataEquality, \
    waitNodeDataInequality
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected, \
    reconnect_node_and_ensure_connected

# noinspection PyUnresolvedReferences
from plenum.test.pool_transactions.conftest import \
    clientAndWallet1, client1, wallet1, client1Connected, looper
from stp_core.loop.eventually import eventually

logger = getlogger()
txnCount = 5


def make_master_replica_lag(node):

    node.nodeIbStasher.delay(ppDelay(1200, 0))
    node.nodeIbStasher.delay(pDelay(1200, 0))
    node.nodeIbStasher.delay(cDelay(1200, 0))


def compare_last_ordered_3pc(node):
    last_ordered_by_master = node.replicas._master_replica.last_ordered_3pc
    comparison_results = {
        compare_3PC_keys(replica.last_ordered_3pc, last_ordered_by_master)
        for replica in node.replicas if not replica.isMaster
    }
    assert len(comparison_results) == 1
    return comparison_results.pop()


def backup_replicas_run_forward(node):
    assert compare_last_ordered_3pc(node) < 0


def replicas_synced(node):
    assert compare_last_ordered_3pc(node) == 0


def test_node_catchup_causes_no_desync(looper, txnPoolNodeSet, client1,
                                       wallet1, client1Connected, monkeypatch):
    """
    Checks that transactions received by catchup do not
    break performance monitoring
    """

    client, wallet = client1, wallet1
    lagging_node = get_any_non_primary_node(txnPoolNodeSet)
    rest_nodes = set(txnPoolNodeSet).difference({lagging_node})

    # Make master replica lagging by hiding all messages sent to it
    make_master_replica_lag(lagging_node)
    monkeypatch.setattr(lagging_node.master_replica,
                        '_request_missing_three_phase_messages',
                        lambda *x, **y: None)

    # Send some requests and check that all replicas except master executed it
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    waitNodeDataInequality(looper, lagging_node, *rest_nodes)
    looper.run(eventually(backup_replicas_run_forward, lagging_node))

    # Disconnect lagging node, send some more requests and start it back
    # After start it should fall in a such state that it needs to make catchup
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            lagging_node,
                                            stopNode=False)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    reconnect_node_and_ensure_connected(looper, txnPoolNodeSet, lagging_node)

    # Check that catchup done
    waitNodeDataEquality(looper, lagging_node, *rest_nodes)

    # Send some more requests to ensure that backup and master replicas
    # are in the same state
    sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 5)
    looper.run(eventually(replicas_synced, lagging_node))

    # Check that master is not considered to be degraded
    assert not lagging_node.monitor.isMasterDegraded()
