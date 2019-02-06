import pytest
from pytest import fixture

from plenum.common.throughput_measurements import RevivalSpikeResistantEMAThroughputMeasurement
from plenum.test.helper import sdk_send_random_and_check

from plenum.test.pool_transactions.conftest import sdk_node_theta_added

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
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 30)
    sdk_node_theta_added(looper, txnPoolNodeSet, tdir, tconf,
                         sdk_pool_handle, sdk_wallet_steward, allPluginsPath)
    looper.runFor(tconf.throughput_measurement_params['window_size'] *
                  tconf.throughput_measurement_params['min_cnt'])
    node = txnPoolNodeSet[0]
    assert all(node.monitor.getThroughput(instance) == 0 for instance in (0, 1, 2))
