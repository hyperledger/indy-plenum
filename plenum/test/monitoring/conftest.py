import pytest

from plenum.test.eventually import eventually
from plenum.test.helper import sendRandomRequest, checkSufficientRepliesRecvd


@pytest.fixture(scope="module")
def requests(looper, client1):
    requests = []
    for i in range(5):
        req = sendRandomRequest(client1)
        looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox, req.reqId, 1,
                              retryWait=1, timeout=5))
        requests.append(req)
    return requests