from plenum.test import waits
from plenum.test.helper import checkViewNoForNodes, sendRandomRequest, \
    waitForSufficientRepliesForRequests, \
    verify_request_not_replied_and_not_ordered
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getRequiredInstances
from plenum.test.view_change.helper import start_stopped_node

TestRunningTimeLimitSec = 200

nodeCount = 5


def stop_node(node_to_stop, looper, pool_nodes):
    disconnect_node_and_ensure_disconnected(looper, pool_nodes, node_to_stop)
    looper.removeProdable(node_to_stop)


def test_quorum_after_f_plus_2_nodes_but_not_primary_turned_off_and_later_on(
        looper, allPluginsPath, tdir, tconf,
        txnPoolNodeSet, wallet1, client1, client1Connected):

    nodes = txnPoolNodeSet

    request1 = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request1])

    stop_node(nodes[4], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[:4], expectedViewNo=0)

    request2 = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request2])

    stop_node(nodes[3], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[:3], expectedViewNo=0)

    request3 = sendRandomRequest(wallet1, client1)
    verify_request_not_replied_and_not_ordered(request3, looper, client1, nodes)

    stop_node(nodes[2], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[:2], expectedViewNo=0)

    request4 = sendRandomRequest(wallet1, client1)
    verify_request_not_replied_and_not_ordered(request4, looper, client1, nodes)

    nodes[4] = start_stopped_node(nodes[4], looper, tconf, tdir, allPluginsPath)
    looper.runFor(waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[:2] + nodes[4:], expectedViewNo=0)

    request5 = sendRandomRequest(wallet1, client1)
    verify_request_not_replied_and_not_ordered(request5, looper, client1, nodes)

    nodes[3] = start_stopped_node(nodes[3], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes[:2] + nodes[3:],
                        numInstances=getRequiredInstances(nodeCount))
    checkViewNoForNodes(nodes[:2] + nodes[3:], expectedViewNo=0)

    request6 = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(
        looper, client1, requests=[request3, request4, request5, request6])

    nodes[2] = start_stopped_node(nodes[2], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes,
                        numInstances=getRequiredInstances(nodeCount))
    checkViewNoForNodes(nodes, expectedViewNo=0)

    request7 = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request7])
