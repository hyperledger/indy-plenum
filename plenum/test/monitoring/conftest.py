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
def decreasedMonitoringTimeouts(tconf, request):
    oldThroughputWindowSize = tconf.ThroughputWindowSize
    oldDashboardUpdateFreq = tconf.DashboardUpdateFreq
    oldLatencyWindowSize = tconf.LatencyWindowSize
    tconf.ThroughputWindowSize = 5
    tconf.LatencyWindowSize = 5
    tconf.DashboardUpdateFreq = 1

    def reset():
        tconf.ThroughputWindowSize = oldThroughputWindowSize
        tconf.LatencyWindowSize = oldLatencyWindowSize
        tconf.DashboardUpdateFreq = oldDashboardUpdateFreq

    request.addfinalizer(reset)
    return tconf
