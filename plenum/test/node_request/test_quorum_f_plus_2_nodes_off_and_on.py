import pytest

from plenum.test import waits
from plenum.test.helper import waitForViewChange, \
    check_request_is_not_returned_to_nodes, checkViewNoForNodes, \
    sendRandomRequest, waitForSufficientRepliesForRequests
from plenum.test.pool_transactions.conftest import looper, clientAndWallet1, \
    client1, wallet1, client1Connected
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getRequiredInstances
from plenum.test.view_change.helper import start_stopped_node

nodeCount = 5


def stop_node(node_to_stop, looper, pool_nodes):
    disconnect_node_and_ensure_disconnected(looper, pool_nodes, node_to_stop)
    looper.removeProdable(node_to_stop)


def verify_request_not_replied_and_not_ordered(request, looper, client, nodes):
    with pytest.raises(AssertionError):
        waitForSufficientRepliesForRequests(looper, client, requests=[request])
    check_request_is_not_returned_to_nodes(looper, nodes, request)


def test_pool_reaches_quorum_after_f_plus_2_nodes_turned_off_and_later_on(
        looper, allPluginsPath, tdir, tconf,
        txnPoolNodeSet, wallet1, client1, client1Connected):

    nodes = txnPoolNodeSet
    initial_view_no = nodes[0].viewNo

    request = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request])

    stop_node(nodes[0], looper, nodes)
    waitForViewChange(looper, nodes[1:], expectedViewNo=initial_view_no + 1)
    ensureElectionsDone(looper, nodes[1:],
                        numInstances=getRequiredInstances(nodeCount))

    request = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request])

    stop_node(nodes[1], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)
    checkViewNoForNodes(nodes[2:], initial_view_no + 1)

    request = sendRandomRequest(wallet1, client1)
    verify_request_not_replied_and_not_ordered(request, looper, client1, nodes)

    stop_node(nodes[2], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)
    checkViewNoForNodes(nodes[3:], initial_view_no + 1)

    request = sendRandomRequest(wallet1, client1)
    verify_request_not_replied_and_not_ordered(request, looper, client1, nodes)

    nodes[2] = start_stopped_node(nodes[2], looper, tconf, tdir, allPluginsPath)
    looper.runFor(waits.expectedPoolElectionTimeout(len(nodes)))

    request = sendRandomRequest(wallet1, client1)
    verify_request_not_replied_and_not_ordered(request, looper, client1, nodes)

    nodes[1] = start_stopped_node(nodes[1], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes[1:],
                        numInstances=getRequiredInstances(nodeCount))
    waitForViewChange(looper, nodes[1:], expectedViewNo=initial_view_no + 1)

    request = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request])

    nodes[0] = start_stopped_node(nodes[0], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes,
                        numInstances=getRequiredInstances(nodeCount))
    waitForViewChange(looper, nodes, expectedViewNo=initial_view_no + 1)

    request = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[request])
