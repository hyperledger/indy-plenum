import pytest

from plenum.common.util import compare_3PC_keys
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import checkNodesConnected
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

logger = getlogger()


def compare_last_ordered_3pc(node):
    last_ordered_by_master = node.replicas._master_replica.last_ordered_3pc
    comparison_results = {
        compare_3PC_keys(replica.last_ordered_3pc, last_ordered_by_master)
        for replica in node.replicas if not replica.isMaster
    }
    assert len(comparison_results) == 1
    return comparison_results.pop()


def replicas_synced(node):
    assert compare_last_ordered_3pc(node) == 0


def test_integration_setup_last_ordered_after_catchup(looper, txnPoolNodeSet,
                                                sdk_wallet_steward,
                                                sdk_wallet_client,
                                                sdk_pool_handle, tdir,
                                                tconf, allPluginsPath):
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    _, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        'EpsilonSteward', 'Epsilon', tdir, tconf,
        allPluginsPath=allPluginsPath)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1])
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    looper.run(eventually(replicas_synced, new_node))
    for node in txnPoolNodeSet:
        for replica in node.replicas:
            assert replica.last_ordered_3pc == (0, 4)
            if not replica.isMaster:
                assert get_count(replica, replica._request_three_phase_msg) == 0
