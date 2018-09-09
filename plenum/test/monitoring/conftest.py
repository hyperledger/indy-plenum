import pytest

from plenum.test.helper import sdk_eval_timeout, sdk_send_random_request, sdk_get_reply


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
