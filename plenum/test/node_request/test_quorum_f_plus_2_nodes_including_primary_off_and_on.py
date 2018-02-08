from plenum.test import waits
from plenum.test.helper import waitForViewChange, checkViewNoForNodes, \
    sendRandomRequest, waitForSufficientRepliesForRequests, \
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


def test_quorum_after_f_plus_2_nodes_including_primary_turned_off_and_later_on(
        looper, allPluginsPath, tdir, tconf,
        txnPoolNodeSet, wallet1, client1, client1Connected):

    nodes = txnPoolNodeSet

    request1 = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request1])

    stop_node(nodes[0], looper, nodes)
    waitForViewChange(looper, nodes[1:], expectedViewNo=1)
    ensureElectionsDone(looper, nodes[1:],
                        numInstances=getRequiredInstances(nodeCount))

    request2 = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request2])

    stop_node(nodes[1], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[2:], expectedViewNo=1)

    request3 = sendRandomRequest(wallet1, client1)
    verify_request_not_replied_and_not_ordered(request3, looper, client1, nodes)

    stop_node(nodes[2], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[3:], expectedViewNo=1)

    request4 = sendRandomRequest(wallet1, client1)
    verify_request_not_replied_and_not_ordered(request4, looper, client1, nodes)

    nodes[2] = start_stopped_node(nodes[2], looper, tconf, tdir, allPluginsPath)
    looper.runFor(waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[3:], expectedViewNo=1)

    request5 = sendRandomRequest(wallet1, client1)
    verify_request_not_replied_and_not_ordered(request5, looper, client1, nodes)

    nodes[1] = start_stopped_node(nodes[1], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes[1:],
                        numInstances=getRequiredInstances(nodeCount))
    checkViewNoForNodes(nodes[1:], expectedViewNo=1)

    request6 = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request6])

    nodes[0] = start_stopped_node(nodes[0], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes,
                        numInstances=getRequiredInstances(nodeCount))
    checkViewNoForNodes(nodes, expectedViewNo=1)

    request7 = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request7])
