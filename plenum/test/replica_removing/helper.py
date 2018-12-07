import sys
from contextlib import contextmanager

from plenum.common.throughput_measurements import RevivalSpikeResistantEMAThroughputMeasurement
from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_send_batches_of_random_and_check, waitForViewChange, \
    acc_monitor
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.eventually import eventually


@contextmanager
def replica_removing(tconf, acc_monitor_enabled, replica_remove_stratgey):
    with acc_monitor(tconf, acc_monitor_enabled=acc_monitor_enabled):
        old_throughput_measurement_class = tconf.throughput_measurement_class
        old_throughput_measurement_params = tconf.throughput_measurement_params
        old_check_time = tconf.PerfCheckFreq
        old_replicas_removing = tconf.REPLICAS_REMOVING_WITH_DEGRADATION

        tconf.throughput_measurement_class = RevivalSpikeResistantEMAThroughputMeasurement
        tconf.throughput_measurement_params = {
            'window_size': 2,
            'min_cnt': 3
        }
        tconf.REPLICAS_REMOVING_WITH_DEGRADATION = replica_remove_stratgey
        tconf.PerfCheckFreq = 5
        yield tconf

        tconf.throughput_measurement_class = old_throughput_measurement_class
        tconf.throughput_measurement_params = old_throughput_measurement_params
        tconf.PerfCheckFreq = old_check_time
        tconf.REPLICAS_REMOVING_WITH_DEGRADATION = old_replicas_removing


def check_replica_removed(node, start_replicas_count, instance_id):
    replicas_count = start_replicas_count - 1
    assert node.replicas.num_replicas == replicas_count
    replicas_lists = [node.replicas.keys(),
                      node.replicas._messages_to_replicas.keys(),
                      node.monitor.numOrderedRequests.keys(),
                      node.monitor.clientAvgReqLatencies.keys(),
                      node.monitor.throughputs.keys(),
                      node.monitor.requestTracker.instances_ids,
                      node.monitor.instances.ids]
    assert all(instance_id not in replicas for replicas in replicas_lists)
    if node.monitor.acc_monitor is not None:
        assert instance_id not in node.monitor.acc_monitor._instances


def do_test_replica_removing_with_backup_degraded(looper,
                                                  txnPoolNodeSet,
                                                  sdk_pool_handle,
                                                  sdk_wallet_client,
                                                  tconf):
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
