import sys
from contextlib import ExitStack

import pytest

from plenum.test.delayers import cDelay
from plenum.test.replica.helper import check_replica_removed
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually
from plenum.test.helper import waitForViewChange, create_new_test_node, \
    sdk_send_batches_of_random_and_check
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected

nodeCount = 7


@pytest.fixture(scope="module")
def update_conf(tconf, tmp):
    old_backup_degraded_logic = tconf.REPLICAS_REMOVING_WITH_DEGRADATION
    old_acc_monitor_enabled = tconf.ACC_MONITOR_ENABLED

    tconf.REPLICAS_REMOVING_WITH_DEGRADATION = tmp["replica_remove"]
    tconf.ACC_MONITOR_ENABLED = tmp["monitor_strategy"]
    tconf.PerfCheckFreq = 5

    yield tconf

    tconf.REPLICAS_REMOVING_WITH_DEGRADATION = old_backup_degraded_logic
    tconf.ACC_MONITOR_ENABLED = old_acc_monitor_enabled


@pytest.fixture(scope="module", params=[{"monitor_strategy": True, "replica_remove": "local"},
                                        {"monitor_strategy": False, "replica_remove": "quorum"},
                                        {"monitor_strategy": False, "replica_remove": "local"},
                                        {"monitor_strategy": True, "replica_remove": "quorum"}])
def txnPoolNodeSet(node_config_helper_class,
                   patchPluginManager,
                   txnPoolNodesLooper,
                   tdirWithPoolTxns,
                   tdirWithDomainTxns,
                   tdir,
                   tconf,
                   poolTxnNodeNames,
                   allPluginsPath,
                   tdirWithNodeKeepInited,
                   testNodeClass,
                   do_post_node_creation,
                   request):
    update_conf(tconf, request.param)
    with ExitStack() as exitStack:
        nodes = []
        for nm in poolTxnNodeNames:
            node = exitStack.enter_context(create_new_test_node(
                testNodeClass, node_config_helper_class, nm, tconf, tdir,
                allPluginsPath))
            do_post_node_creation(node)
            txnPoolNodesLooper.add(node)
            nodes.append(node)
        txnPoolNodesLooper.run(checkNodesConnected(nodes))
        ensureElectionsDone(looper=txnPoolNodesLooper, nodes=nodes)
        yield nodes
        for node in nodes:
            txnPoolNodesLooper.removeProdable(node)


def test_replica_removing_with_backup_degraded(looper,
                                               txnPoolNodeSet,
                                               sdk_pool_handle,
                                               sdk_wallet_client,
                                               tconf,
                                               tdir,
                                               allPluginsPath):
    """
    1. Start backup degraded.
    2. Check that degraded replicas
    3. Start View Change.
    4. Check that all replicas were restored.
    """
    instance_to_remove = 1
    view_no = txnPoolNodeSet[0].viewNo
    start_replicas_count = txnPoolNodeSet[0].replicas.num_replicas
    for node in txnPoolNodeSet:
        node.backup_instance_faulty_processor.on_backup_degradation([instance_to_remove])

    # check that replicas were removed
    def check_replica_removed_on_all_nodes():
        for node in txnPoolNodeSet:
            check_replica_removed(node,
                                  start_replicas_count,
                                  instance_to_remove)

    looper.run(eventually(check_replica_removed_on_all_nodes, timeout=60))
    for node in txnPoolNodeSet:
        assert not node.monitor.isMasterDegraded()
        assert len(node.requests) == 0
    # start View Change
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=view_no + 1,
                      customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    # check that all replicas were restored
    assert all(start_replicas_count == node.replicas.num_replicas
               for node in txnPoolNodeSet)
