import functools

import pytest

from plenum.server.instances import Instances
from plenum.server.monitor import Monitor, LatencyMeasurement, MedianHighStrategy
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
    monitor.getLatencies = functools.partial(Monitor.getLatencies, monitor)
    monitor.isMasterAvgReqLatencyTooHigh = functools.partial(Monitor.isMasterAvgReqLatencyTooHigh, monitor)
    return monitor


def test_low_median_avg_latencies(fake_monitor, tconf):
    identifier = "SomeClient"
    monitor = fake_monitor
    master_lat = 120
    req_count = 1000
    # Filling for case, when the most of backup latencies are small and several is very big
    # Case is [120, 101, 1200, 1200, 1200]
    monitor.clientAvgReqLatencies[0].avg_latencies[identifier] = (req_count, master_lat)
    monitor.clientAvgReqLatencies[1].avg_latencies[identifier] = (req_count, master_lat - tconf.OMEGA + 1)
    for i in range(2, NUM_OF_REPLICAS):
        monitor.clientAvgReqLatencies[i].avg_latencies[identifier] = (req_count, 10 * master_lat)
    assert monitor.isMasterAvgReqLatencyTooHigh() is False


def test_medium_median_avg_latencies(fake_monitor, tconf):
    identifier = "SomeClient"
    monitor = fake_monitor
    master_lat = 120
    req_count = 1000
    # Filling for case, when the most of backup latencies are similar with master and several is very big
    # Case is [120, 1, 101, 101, 1200]
    monitor.clientAvgReqLatencies[0].avg_latencies[identifier] = (req_count, master_lat)
    monitor.clientAvgReqLatencies[1].avg_latencies[identifier] = (req_count, 1)
    monitor.clientAvgReqLatencies[-1].avg_latencies[identifier] = (req_count, 10 * master_lat)
    for i in range(2, NUM_OF_REPLICAS - 1):
        monitor.clientAvgReqLatencies[i].avg_latencies[identifier] = (req_count, master_lat - tconf.OMEGA + 1)
    assert monitor.isMasterAvgReqLatencyTooHigh() is False


def test_high_median_avg_latencies(fake_monitor, tconf):
    identifier = "SomeClient"
    monitor = fake_monitor
    master_lat = 120
    req_count = 1000
    # Filling for case, when the most of backup latencies a similar with master,
    # but there is a some very big values
    # Case is [120, 1, 101, 101, 1]
    monitor.clientAvgReqLatencies[0].avg_latencies[identifier] = (req_count, master_lat)
    monitor.clientAvgReqLatencies[1].avg_latencies[identifier] = (req_count, 1)
    monitor.clientAvgReqLatencies[-1].avg_latencies[identifier] = (req_count, 1)
    for i in range(2, NUM_OF_REPLICAS - 1):
        monitor.clientAvgReqLatencies[i].avg_latencies[identifier] = (req_count, master_lat - tconf.OMEGA + 1)
    assert monitor.isMasterAvgReqLatencyTooHigh() is False


def test_trigger_view_change(fake_monitor, tconf):
    identifier = "SomeClient"
    monitor = fake_monitor
    # Filling for case, when the master's average latency is not acceptable
    # Case is [120, 99, 99, 99, 99]
    master_lat = 120
    req_count = 1000
    monitor.clientAvgReqLatencies[0].avg_latencies[identifier] = (req_count, master_lat)
    for i in range(1, NUM_OF_REPLICAS):
        monitor.clientAvgReqLatencies[i].avg_latencies[identifier] = (req_count, master_lat - tconf.OMEGA - 1)
    assert monitor.isMasterAvgReqLatencyTooHigh()
