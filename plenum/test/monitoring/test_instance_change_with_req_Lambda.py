from functools import partial
import pytest

from plenum.common.types import PrePrepare, f
from plenum.test.eventually import eventually
from plenum.test.helper import checkViewNoForNodes, getPrimaryReplica, \
    sendReqsToNodesAndVerifySuffReplies, getAllReturnVals
from plenum.common.util import adict

nodeCount = 7
whitelist = ["discarding message"]

"""
We start with a 7 node consensus pool
Lets call the master instance's primary replica P
Drop threshold `Delta` to very low so that view change does not happen
because of drop in throughput. Another approach could have been to send a lot of
requests and delay only one request such that the ratio of throughputs does not
drop below `Delta`.
Make P faulty: slow to send PRE-PREPAREs to only a specific request
verify that throughput has dropped
Verify a view change happens
"""


@pytest.fixture(scope="module")
def setup(looper, startedNodes, up, wallet1, client1):
    # Get the master replica of the master protocol instance
    P = getPrimaryReplica(startedNodes)

    # Make `Delta` small enough so throughput check passes.
    for node in startedNodes:
        node.monitor.Delta = .001

    slowRequest = None

    # make P (primary replica on master) faulty, i.e., slow to send
    # PRE-PREPARE for a specific client request only
    def by65SpecificPrePrepare(msg):
        nonlocal slowRequest
        if isinstance(msg, PrePrepare) and slowRequest is None:
            slowRequest = getattr(msg, f.REQ_ID.nm)
            return 65

    P.outBoxTestStasher.delay(by65SpecificPrePrepare)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1,
                                        numReqs=5, timeoutPerReq=80)

    return adict(nodes=startedNodes)


def testInstChangeWithMoreReqLat(looper, setup):
    nodes = setup.nodes
    for node in nodes:
        node.checkPerformance()
        assert any(getAllReturnVals(node.monitor,
                                    node.monitor.isMasterReqLatencyTooHigh))
    looper.run(eventually(partial(checkViewNoForNodes, nodes, 1),
                          retryWait=1, timeout=20))
