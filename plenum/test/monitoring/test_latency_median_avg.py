import functools

import pytest

from plenum.server.instances import Instances
from plenum.server.monitor import Monitor
from plenum.common.measurements import LatencyMeasurement
from plenum.common.average_strategies import MedianHighStrategy
from plenum.test.testing_utils import FakeSomething

NUM_OF_REPLICAS = 5


@pytest.fixture(scope='function')
def fake_monitor(tconf):
    latencies = []
    instances = Instances()
    for i in range(NUM_OF_REPLICAS):
        latencies.append(LatencyMeasurement())
        instances.add()
    monitor = FakeSomething(
        instances=instances,
        Omega=tconf.OMEGA,
        clientAvgReqLatencies=latencies,
        latency_avg_strategy_cls=MedianHighStrategy,
        )
    monitor.getLatency = functools.partial(Monitor.getLatency, monitor)
    monitor.isMasterAvgReqLatencyTooHigh = functools.partial(Monitor.isMasterAvgReqLatencyTooHigh, monitor)
    return monitor


def test_low_median_avg_latencies(fake_monitor, tconf):
    identifier = "SomeClient"
    monitor = fake_monitor
    master_lat = 120
    # Filling for case, when the most of backup latencies are small and several is very big
    # Case is [120, 101, 1200, 1200, 1200]
    latencies = [120, 101, 1200, 1200, 1200]
    avg_strategy = tconf.AvgStrategyForBackups()
    assert master_lat - avg_strategy.get_avg(latencies) < tconf.OMEGA


def test_medium_median_avg_latencies(tconf):
    master_lat = 120
    # Filling for case, when the most of backup latencies are similar with master and several is very big
    # Case is [120, 1, 101, 101, 1200]
    latencies = [120, 1, 101, 101, 1200]
    avg_strategy = tconf.AvgStrategyForBackups()
    assert master_lat - avg_strategy.get_avg(latencies) < tconf.OMEGA


def test_high_median_avg_latencies(tconf):
    master_lat = 120
    # Filling for case, when the most of backup latencies a similar with master,
    # but there is a some very big values
    # Case is [120, 1, 101, 101, 1]
    latencies = [120, 1, 101, 101, 1]
    avg_strategy = tconf.AvgStrategyForBackups()
    assert master_lat - avg_strategy.get_avg(latencies) < tconf.OMEGA


def test_trigger_view_change(tconf):
    # Filling for case, when the master's average latency is not acceptable
    # Case is [120, 99, 99, 99, 99]
    master_lat = 120
    latencies = [120, 99, 99, 99, 99]
    avg_strategy = tconf.AvgStrategyForBackups()
    assert master_lat - avg_strategy.get_avg(latencies) > tconf.OMEGA
