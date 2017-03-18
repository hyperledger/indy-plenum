import pytest

from plenum.common.eventually import eventually
from plenum.test.helper import sendRandomRequest, checkSufficientRepliesRecvd


@pytest.fixture(scope="module")
def requests(looper, wallet1, client1):
    requests = []
    for i in range(5):
        req = sendRandomRequest(wallet1, client1)
        looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox,
                              req.reqId, 1, retryWait=1, timeout=5))
        requests.append(req)
    return requests
