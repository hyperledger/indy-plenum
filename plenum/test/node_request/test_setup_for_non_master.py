import pytest

from plenum.common.util import compare_3PC_keys
from plenum.test.helper import sdk_send_random_and_check, waitForViewChange
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import checkNodesConnected
from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually

logger = getlogger()


def compare_last_ordered_3pc(node, last_ordered):
    comparison_results = {
        compare_3PC_keys(replica.last_ordered_3pc, last_ordered)
        for replica in node.replicas.values() if not replica.isMaster
    }
    assert len(comparison_results) == 1
    return comparison_results.pop()


def backup_replicas_synced(nodes, last_ordered):
    for node in nodes:
        assert compare_last_ordered_3pc(node, last_ordered) == 0


def test_integration_setup_last_ordered_after_catchup(looper, txnPoolNodeSet,
                                                sdk_wallet_steward,
                                                sdk_wallet_client,
                                                sdk_pool_handle, tdir,
                                                tconf, allPluginsPath):
    start_view_no = txnPoolNodeSet[0].viewNo
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    _, new_node = sdk_add_new_steward_and_node(
        looper, sdk_pool_handle, sdk_wallet_steward,
        'EpsilonSteward', 'Epsilon', tdir, tconf,
        allPluginsPath=allPluginsPath)
    txnPoolNodeSet.append(new_node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=start_view_no + 1)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:-1],
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 1)
    looper.run(eventually(backup_replicas_synced, txnPoolNodeSet, (start_view_no + 1, 2)))
    for node in txnPoolNodeSet:
        for replica in node.replicas.values():
            if not replica.isMaster:
                assert get_count(replica._ordering_service,
                                 replica._ordering_service._request_three_phase_msg) == 0
            else:
                assert replica.last_ordered_3pc == (1, 5)
