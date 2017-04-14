import pytest

from plenum.test.helper import sendRandomRequest, \
    waitForSufficientRepliesForRequests


@pytest.fixture(scope="module")
def requests(looper, wallet1, client1):
    requests = []
    for i in range(5):
        req = sendRandomRequest(wallet1, client1)
        waitForSufficientRepliesForRequests(looper, client1,
                                            requests=[req], fVal=1)
        requests.append(req)
    return requests
