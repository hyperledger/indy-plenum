import pytest

from plenum.common.request import Request
from plenum.test import waits
from plenum.test.helper import waitForViewChange, sdk_send_random_and_check, \
    sdk_send_random_request, sdk_eval_timeout, sdk_get_reply, sdk_check_reply, \
    check_request_is_not_returned_to_nodes, checkViewNoForNodes
from plenum.test.pool_transactions.conftest import looper
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getRequiredInstances
from plenum.test.view_change.helper import start_stopped_node

nodeCount = 5


def stop_node(node_to_stop, looper, pool_nodes):
    disconnect_node_and_ensure_disconnected(looper, pool_nodes, node_to_stop)
    looper.removeProdable(node_to_stop)


def ensure_request_not_ordered_and_not_replied(sdk_req_resp, looper, nodes):
    sdk_req_reply = sdk_get_reply(looper, sdk_req_resp,
                              timeout=sdk_eval_timeout(1, len(nodes)))
    with pytest.raises(AssertionError,
                       match=r'Got no confirmed result for request.*'):
        sdk_check_reply(sdk_req_reply)
    req, _ = sdk_req_resp
    check_request_is_not_returned_to_nodes(looper, nodes, Request(req))


def test_pool_reaches_quorum_after_more_than_f_plus_1_nodes_were_off_and_later_on(
        looper, allPluginsPath, tdir, tconf,
        txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):

    nodes = txnPoolNodeSet
    initial_view_no = nodes[0].viewNo

    sdk_send_random_and_check(looper, nodes,
                              sdk_pool_handle, sdk_wallet_client, 1)

    stop_node(nodes[0], looper, nodes)
    waitForViewChange(looper, nodes[1:], expectedViewNo=initial_view_no + 1)
    ensureElectionsDone(looper, nodes[1:],
                        numInstances=getRequiredInstances(nodeCount))

    sdk_send_random_and_check(looper, nodes,
                              sdk_pool_handle, sdk_wallet_client, 1)

    stop_node(nodes[1], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)
    checkViewNoForNodes(nodes[2:], initial_view_no + 1)

    sdk_req_resp = sdk_send_random_request(looper, sdk_pool_handle,
                                           sdk_wallet_client)
    ensure_request_not_ordered_and_not_replied(sdk_req_resp, looper, nodes)

    stop_node(nodes[2], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection + 2)
    checkViewNoForNodes(nodes[3:], initial_view_no + 1)

    sdk_req_resp = sdk_send_random_request(looper, sdk_pool_handle,
                                           sdk_wallet_client)
    ensure_request_not_ordered_and_not_replied(sdk_req_resp, looper, nodes)

    nodes[2] = start_stopped_node(nodes[2], looper, tconf, tdir, allPluginsPath)
    looper.runFor(waits.expectedPoolElectionTimeout(len(nodes)))

    sdk_req_resp = sdk_send_random_request(looper, sdk_pool_handle,
                                           sdk_wallet_client)
    ensure_request_not_ordered_and_not_replied(sdk_req_resp, looper, nodes)

    nodes[1] = start_stopped_node(nodes[1], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes[1:],
                        numInstances=getRequiredInstances(nodeCount))
    waitForViewChange(looper, nodes[1:], expectedViewNo=initial_view_no + 1)

    sdk_send_random_and_check(looper, nodes,
                              sdk_pool_handle, sdk_wallet_client, 1)

    nodes[0] = start_stopped_node(nodes[0], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes,
                        numInstances=getRequiredInstances(nodeCount))
    waitForViewChange(looper, nodes, expectedViewNo=initial_view_no + 1)

    sdk_send_random_and_check(looper, nodes,
                              sdk_pool_handle, sdk_wallet_client, 1)
