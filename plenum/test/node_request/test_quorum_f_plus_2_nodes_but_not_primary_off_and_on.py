import pytest

from plenum.common.exceptions import PoolLedgerTimeoutException
from plenum.test import waits
from plenum.test.helper import checkViewNoForNodes, \
    sdk_send_random_and_check, sdk_send_random_requests, sdk_get_replies, \
    sdk_check_reply
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getRequiredInstances
from plenum.test.view_change.helper import start_stopped_node

# TODO: restore previous value (200) after sdk will
# have api call to change its timeout
TestRunningTimeLimitSec = 400

nodeCount = 5
whitelist = ['Consensus for ReqId:']


def stop_node(node_to_stop, looper, pool_nodes):
    disconnect_node_and_ensure_disconnected(looper, pool_nodes, node_to_stop)
    looper.removeProdable(node_to_stop)


def test_quorum_after_f_plus_2_nodes_but_not_primary_turned_off_and_later_on(
        looper, allPluginsPath, tdir, tconf,
        txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    nodes = txnPoolNodeSet

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              1)

    stop_node(nodes[4], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[:4], expectedViewNo=0)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              1)

    stop_node(nodes[3], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[:3], expectedViewNo=0)

    sdk_reqs3 = sdk_send_random_requests(looper,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         1)
    with pytest.raises(PoolLedgerTimeoutException):
        req_res = sdk_get_replies(looper, sdk_reqs3)
        sdk_check_reply(req_res[0])

    stop_node(nodes[2], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[:2], expectedViewNo=0)

    sdk_reqs4 = sdk_send_random_requests(looper,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         1)
    with pytest.raises(PoolLedgerTimeoutException):
        req_res = sdk_get_replies(looper, sdk_reqs4)
        sdk_check_reply(req_res[0])

    nodes[4] = start_stopped_node(nodes[4], looper, tconf, tdir, allPluginsPath)
    looper.runFor(waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[:2] + nodes[4:], expectedViewNo=0)

    sdk_reqs5 = sdk_send_random_requests(looper,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         1)
    with pytest.raises(PoolLedgerTimeoutException):
        req_res = sdk_get_replies(looper, sdk_reqs5)
        sdk_check_reply(req_res[0])

    nodes[3] = start_stopped_node(nodes[3], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes[:2] + nodes[3:],
                        instances_list=range(getRequiredInstances(nodeCount)))
    checkViewNoForNodes(nodes[:2] + nodes[3:], expectedViewNo=0)

    sdk_reqs6 = sdk_send_random_requests(looper,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         1)
    sdk_get_replies(looper, sdk_reqs6)

    nodes[2] = start_stopped_node(nodes[2], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes,
                        instances_list=range(getRequiredInstances(nodeCount)))
    checkViewNoForNodes(nodes, expectedViewNo=0)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              1)
