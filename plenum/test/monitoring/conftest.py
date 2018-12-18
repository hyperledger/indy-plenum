import functools

import pytest

from plenum.common.average_strategies import MedianLowStrategy
from plenum.server.instances import Instances
from plenum.server.monitor import Monitor
from plenum.test.helper import sdk_eval_timeout, sdk_send_random_request, sdk_get_reply
from plenum.test.testing_utils import FakeSomething


@pytest.fixture()
def requests(looper, sdk_wallet_client, sdk_pool_handle):
    requests = []
    for i in range(5):
        req = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)
        req, _ = sdk_get_reply(looper, req, timeout=sdk_eval_timeout(1, 4))
        requests.append(req)
    return requests


@pytest.fixture
def decreasedMonitoringTimeouts(tconf, request):
    oldDashboardUpdateFreq = tconf.DashboardUpdateFreq
    tconf.DashboardUpdateFreq = 1

    def reset():
        tconf.DashboardUpdateFreq = oldDashboardUpdateFreq

    request.addfinalizer(reset)
    return tconf


@pytest.fixture(scope='function')
def fake_monitor(tconf):
    def getThroughput(self, instId):
        return self.throughputs[instId].throughput

    throughputs = dict()
    instances = Instances()
    num_of_replicas = 5
    for i in range(num_of_replicas):
        throughputs[i] = Monitor.create_throughput_measurement(tconf)
        instances.add(i)
    monitor = FakeSomething(
        throughputs=throughputs,
        instances=instances,
        Delta=tconf.DELTA,
        throughput_avg_strategy_cls=MedianLowStrategy,
        )
    monitor.numOrderedRequests = dict()
    for i in range(num_of_replicas):
        monitor.numOrderedRequests[i] = (100, 100)
    monitor.getThroughputs = functools.partial(Monitor.getThroughputs, monitor)
    monitor.getThroughput = functools.partial(getThroughput, monitor)
    monitor.getInstanceMetrics = functools.partial(Monitor.getInstanceMetrics, monitor)
    monitor.instance_throughput_ratio = functools.partial(Monitor.instance_throughput_ratio, monitor)
    monitor.is_instance_throughput_too_low = functools.partial(Monitor.is_instance_throughput_too_low, monitor)
    monitor.addInstance = functools.partial(Monitor.addInstance, monitor)
    return monitor