import pytest
from pytest import fixture

from plenum.common.throughput_measurements import RevivalSpikeResistantEMAThroughputMeasurement
from plenum.common.util import getMaxFailures
from plenum.test.helper import sdk_send_random_and_check, assertExp
from plenum.test.node_catchup.helper import waitNodeDataEquality

from plenum.test.pool_transactions.conftest import sdk_node_theta_added
from stp_core.loop.eventually import eventually

nodeCount = 6


@pytest.fixture(scope="module")
def tconf(tconf):
    old_throughput_measurement_class = tconf.throughput_measurement_class
    old_throughput_measurement_params = tconf.throughput_measurement_params

    tconf.throughput_measurement_class = RevivalSpikeResistantEMAThroughputMeasurement
    tconf.throughput_measurement_params = {
        'window_size': 5,
        'min_cnt': 2
    }

    yield tconf

    tconf.throughput_measurement_class = old_throughput_measurement_class
    tconf.throughput_measurement_params = old_throughput_measurement_params


def test_monitor_reset_after_replica_addition(looper, sdk_pool_handle, txnPoolNodeSet,
                                              sdk_wallet_steward, tdir, tconf, allPluginsPath):
    view_no = txnPoolNodeSet[-1].viewNo
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 30)
    waitNodeDataEquality(looper, *txnPoolNodeSet)
    last_ordered = txnPoolNodeSet[-1].master_last_ordered_3PC

    sdk_node_theta_added(looper, txnPoolNodeSet, tdir, tconf,
                         sdk_pool_handle, sdk_wallet_steward, allPluginsPath)
    looper.runFor(tconf.throughput_measurement_params['window_size'] *
                  tconf.throughput_measurement_params['min_cnt'])
    node = txnPoolNodeSet[0]
    assert all(node.monitor.getThroughput(instance) == 0 for instance in (0, 1, 2))

    looper.run(eventually(lambda: assertExp(n.viewNo == view_no for n in txnPoolNodeSet)))
    waitNodeDataEquality(looper, *txnPoolNodeSet, exclude_from_check=['check_last_ordered_3pc_backup'])
