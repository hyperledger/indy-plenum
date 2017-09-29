import pytest

from plenum.test.helper import sendRandomRequest, \
    waitForSufficientRepliesForRequests


@pytest.fixture(scope="module")
def requests(looper, wallet1, client1):
    requests = []
    for i in range(5):
        req = sendRandomRequest(wallet1, client1)
        waitForSufficientRepliesForRequests(looper, client1, requests=[req])
        requests.append(req)
    return requests


@pytest.fixture
def decreasedMonitoringTimeouts(conf, request):
    oldThroughputWindowSize = conf.ThroughputWindowSize
    oldDashboardUpdateFreq = conf.DashboardUpdateFreq
    oldLatencyWindowSize = conf.LatencyWindowSize
    conf.ThroughputWindowSize = 5
    conf.LatencyWindowSize = 5
    conf.DashboardUpdateFreq = 1

    def reset():
        conf.ThroughputWindowSize = oldThroughputWindowSize
        conf.LatencyWindowSize = oldLatencyWindowSize
        conf.DashboardUpdateFreq = oldDashboardUpdateFreq

    request.addfinalizer(reset)
    return conf
