import pytest

from plenum.common.exceptions import PoolLedgerTimeoutException
from plenum.test import waits
from plenum.test.helper import waitForViewChange, checkViewNoForNodes, \
    sdk_send_random_and_check, sdk_send_random_requests, sdk_get_replies, \
    sdk_check_reply, sdk_eval_timeout
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.test_node import ensureElectionsDone, getRequiredInstances
from plenum.test.view_change.helper import start_stopped_node

TestRunningTimeLimitSec = 200

nodeCount = 5

whitelist = ['Consensus for ReqId:']


def stop_node(node_to_stop, looper, pool_nodes):
    disconnect_node_and_ensure_disconnected(looper, pool_nodes, node_to_stop)
    looper.removeProdable(node_to_stop)


def test_quorum_after_f_plus_2_nodes_including_primary_turned_off_and_later_on(
        looper, allPluginsPath, tdir, tconf,
        txnPoolNodeSet,
        sdk_pool_handle,
        sdk_wallet_client):
    timeout = sdk_eval_timeout(1, len(txnPoolNodeSet))
    nodes = txnPoolNodeSet

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              1)

    stop_node(nodes[0], looper, nodes)
    waitForViewChange(looper, nodes[1:], expectedViewNo=1)
    ensureElectionsDone(looper, nodes[1:],
                        instances_list=range(getRequiredInstances(nodeCount)))

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              1)

    stop_node(nodes[1], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[2:], expectedViewNo=1)

    sdk_reqs3 = sdk_send_random_requests(looper,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         1)
    with pytest.raises(PoolLedgerTimeoutException):
        req_res = sdk_get_replies(looper, sdk_reqs3, timeout=timeout)
        sdk_check_reply(req_res[0])

    stop_node(nodes[2], looper, nodes)
    looper.runFor(tconf.ToleratePrimaryDisconnection +
                  waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[3:], expectedViewNo=1)

    sdk_reqs4 = sdk_send_random_requests(looper,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         1)
    with pytest.raises(PoolLedgerTimeoutException):
        req_res = sdk_get_replies(looper, sdk_reqs4, timeout=timeout)
        sdk_check_reply(req_res[0])

    nodes[2] = start_stopped_node(nodes[2], looper, tconf, tdir, allPluginsPath)
    looper.runFor(waits.expectedPoolElectionTimeout(len(nodes)))
    checkViewNoForNodes(nodes[3:], expectedViewNo=1)

    sdk_reqs5 = sdk_send_random_requests(looper,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         1)
    with pytest.raises(PoolLedgerTimeoutException):
        req_res = sdk_get_replies(looper, sdk_reqs5, timeout=timeout)
        sdk_check_reply(req_res[0])

    nodes[1] = start_stopped_node(nodes[1], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes[1:],
                        instances_list=range(getRequiredInstances(nodeCount)),
                        customTimeout=60)
    checkViewNoForNodes(nodes[1:], expectedViewNo=1)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              1)

    nodes[0] = start_stopped_node(nodes[0], looper, tconf, tdir, allPluginsPath)
    ensureElectionsDone(looper, nodes,
                        instances_list=range(getRequiredInstances(nodeCount)),
                        customTimeout=60)
    checkViewNoForNodes(nodes, expectedViewNo=1)

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              1)
