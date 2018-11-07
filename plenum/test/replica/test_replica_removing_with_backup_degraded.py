import functools
import sys
from contextlib import ExitStack

import pytest

from plenum.common.throughput_measurements import RevivalSpikeResistantEMAThroughputMeasurement
from plenum.server.monitor import Monitor
from plenum.test.delayers import cDelay
from plenum.test.replica.helper import check_replica_removed
from plenum.test.stasher import delay_rules
from plenum.test.testing_utils import FakeSomething
from stp_core.loop.eventually import eventually
from plenum.test.helper import waitForViewChange, create_new_test_node, sdk_send_batches_of_random_and_check
from plenum.test.test_node import ensureElectionsDone, checkNodesConnected


@pytest.fixture(scope="module", params=[{"monitor_strategy": False, "replica_remove": "local"},
                                        {"monitor_strategy": False, "replica_remove": "quorum"},
                                        {"monitor_strategy": True, "replica_remove": "local"},
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


def update_conf(tconf, tmp):
    tconf.throughput_measurement_class = RevivalSpikeResistantEMAThroughputMeasurement
    tconf.throughput_measurement_params = {
        'window_size': 2,
        'min_cnt': 3
    }
    tconf.ACC_MONITOR_TIMEOUT = 3
    tconf.ACC_MONITOR_TXN_DELTA_K = 0
    tconf.REPLICAS_REMOVING_WITH_DEGRADATION = tmp["replica_remove"]
    tconf.ACC_MONITOR_ENABLED = tmp["monitor_strategy"]
    tconf.PerfCheckFreq = 5
    return tconf


def test_replica_removing_with_backup_degraded(looper,
                                               txnPoolNodeSet,
                                               sdk_pool_handle,
                                               sdk_wallet_client,
                                               sdk_wallet_steward,
                                               tconf,
                                               tdir,
                                               allPluginsPath):
    """
      Node will change view even though it does not find the master to be degraded
      when a quorum of nodes agree that master performance degraded
      """

    start_replicas_count = txnPoolNodeSet[0].replicas.num_replicas
    view_no = txnPoolNodeSet[0].viewNo
    instance_to_remove = 1
    stashers = [node.nodeIbStasher for node in txnPoolNodeSet]
    with delay_rules(stashers, cDelay(delay=sys.maxsize, instId=instance_to_remove)):
        sdk_send_batches_of_random_and_check(looper,
                                             txnPoolNodeSet,
                                             sdk_pool_handle,
                                             sdk_wallet_client,
                                             num_reqs=10,
                                             num_batches=5)

        # check that replicas were removed
        def check_replica_removed_on_all_nodes(inst_id=instance_to_remove):
            for n in txnPoolNodeSet:
                check_replica_removed(n,
                                      start_replicas_count,
                                      inst_id)
                assert not n.monitor.isMasterDegraded()

        looper.run(eventually(check_replica_removed_on_all_nodes, timeout=120))

    # start View Change
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=view_no + 1,
                      customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    # check that all replicas were restored
    assert all(start_replicas_count == node.replicas.num_replicas
               for node in txnPoolNodeSet)


def test_replica_not_degraded_with_too_high_latency():
    monitor = FakeSomething(
        is_instance_avg_req_latency_too_high=lambda a: True,
        is_instance_throughput_too_low=lambda a: False,
        acc_monitor=None,
        instances=FakeSomething(backupIds=[1, 2, 3]))
    monitor.areBackupsDegraded = functools.partial(Monitor.areBackupsDegraded, monitor)

    assert not monitor.areBackupsDegraded()
