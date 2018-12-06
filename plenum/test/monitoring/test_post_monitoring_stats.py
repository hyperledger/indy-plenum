import pytest

from plenum.server.monitor import Monitor
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data

WIND_SIZE = 5
MIN_CNT = 2


@pytest.fixture(scope='module')
def tconf(tconf):
    old_val = tconf.Max3PCBatchSize
    old_throughput_measurement_params = tconf.throughput_measurement_params

    tconf.Max3PCBatchSize = 1
    tconf.throughput_measurement_params = {
        'window_size': WIND_SIZE,
        'min_cnt': MIN_CNT
    }

    yield tconf
    tconf.Max3PCBatchSize = old_val
    tconf.throughput_measurement_params = old_throughput_measurement_params


def testPostingThroughput(postingStatsEnabled,
                          decreasedMonitoringTimeouts,
                          looper,
                          txnPoolNodeSet,
                          sdk_wallet_client, sdk_pool_handle):
    config = decreasedMonitoringTimeouts
    reqCount = 10
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              reqCount)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    looper.runFor(WIND_SIZE * MIN_CNT)

    for node in txnPoolNodeSet:
        assert node.monitor.highResThroughput > 0
        assert node.monitor.totalRequests == reqCount
        # TODO: Add implementation to actually call firebase plugin
        # and test if firebase plugin is sending total request count
        # if node is primary

    looper.runFor(config.DashboardUpdateFreq)

    for node in txnPoolNodeSet:
        node.monitor.spylog.count(Monitor.sendThroughput.__name__) > 0


def testPostingLatency(postingStatsEnabled,
                       decreasedMonitoringTimeouts,
                       looper,
                       txnPoolNodeSet,
                       sdk_wallet_client, sdk_pool_handle):
    config = decreasedMonitoringTimeouts
    reqCount = 10
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              reqCount)

    for node in txnPoolNodeSet:
        assert node.monitor.masterLatency > 0
        assert node.monitor.avgBackupLatency > 0

    looper.runFor(config.DashboardUpdateFreq)

    for node in txnPoolNodeSet:
        node.monitor.spylog.count(Monitor.sendLatencies.__name__) > 0
