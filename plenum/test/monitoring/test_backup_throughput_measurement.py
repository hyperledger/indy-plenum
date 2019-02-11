from pytest import fixture

from plenum.common.average_strategies import MedianLowStrategy, MedianMediumStrategy
from plenum.common.throughput_measurements import RevivalSpikeResistantEMAThroughputMeasurement
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.pool_transactions.helper import sdk_pool_refresh
from stp_core.loop.eventually import eventually

nodeCount = 8


@fixture(scope='module')
def tconf(tconf):
    old_throughput_measurement_class = tconf.throughput_measurement_class
    old_throughput_averaging_strategy_class = tconf.throughput_averaging_strategy_class
    old_backup_throughput_averaging_strategy_class = tconf.backup_throughput_averaging_strategy_class
    oldPerfCheckFreq = tconf.PerfCheckFreq
    old_throughput_measurement_params = tconf.throughput_measurement_params

    tconf.throughput_measurement_class = RevivalSpikeResistantEMAThroughputMeasurement
    tconf.throughput_averaging_strategy_class = MedianLowStrategy
    tconf.backup_throughput_averaging_strategy_class = MedianMediumStrategy
    tconf.PerfCheckFreq = 1
    tconf.throughput_measurement_params = {
        'window_size': 5,
        'min_cnt': 2
    }

    yield tconf

    tconf.throughput_measurement_class = old_throughput_measurement_class
    tconf.throughput_averaging_strategy_class = old_throughput_averaging_strategy_class
    tconf.backup_throughput_averaging_strategy_class = old_backup_throughput_averaging_strategy_class
    tconf.PerfCheckFreq = oldPerfCheckFreq
    tconf.throughput_measurement_params = old_throughput_measurement_params


def test_backup_throughput_measurement(looper, sdk_pool_handle, txnPoolNodeSet,
                                       sdk_wallet_steward, tdir, tconf, allPluginsPath):
    # 8 nodes, so f == 2 and replicas == 3
    looper.runFor(tconf.throughput_measurement_params['window_size'] *
                  tconf.throughput_measurement_params['min_cnt'])

    # Send some txns
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 1)

    # Stop backup primaries
    for node in txnPoolNodeSet[1:3]:
        node.cleanupOnStopping = False
        looper.removeProdable(node)
        node.stop()

    # Send more txns so that master replica got more throughput
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, int(2 / tconf.DELTA))

    def chk():
        assert len(txnPoolNodeSet[0].monitor.areBackupsDegraded()) == 2

    looper.run(eventually(chk, retryWait=1,
                          timeout=int(tconf.throughput_measurement_params['window_size'] + 2)))
