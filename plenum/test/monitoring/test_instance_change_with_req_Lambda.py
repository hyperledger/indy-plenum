import pytest

from plenum.common.messages.node_messages import PrePrepare
from stp_core.common.util import adict
from plenum.test.helper import waitForViewChange, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import getPrimaryReplica
from plenum.test.spy_helpers import getAllReturnVals

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


@pytest.fixture('module')
def setup(looper, tconf, startedNodes, up, wallet1, client1):
    sendReqsToNodesAndVerifySuffReplies(looper,
                                        wallet1,
                                        client1,
                                        numReqs=5)
    # Get the master replica of the master protocol instance
    P = getPrimaryReplica(startedNodes)

    # set LAMBDA smaller than the production config to make the test faster
    testLambda = 30
    delay_by = testLambda + 5

    for node in startedNodes:
        # Make `Delta` small enough so throughput check passes.
        node.monitor.Delta = .001
        node.monitor.Lambda = testLambda
        for r in node.replicas:
            r.config.ACCEPTABLE_DEVIATION_PREPREPARE_SECS += delay_by

    slowed_request = False

    # make P (primary replica on master) faulty, i.e., slow to send
    # PRE-PREPARE for a specific client request only
    def specificPrePrepare(msg):
        nonlocal slowed_request
        if isinstance(msg, PrePrepare) and slowed_request is False:
            slowed_request = True
            return delay_by  # just more that LAMBDA

    P.outBoxTestStasher.delay(specificPrePrepare)
    # TODO select or create a timeout for this case in 'waits'
    sendReqsToNodesAndVerifySuffReplies(
        looper,
        wallet1,
        client1,
        numReqs=5,
        customTimeoutPerReq=tconf.TestRunningTimeLimitSec)

    return adict(nodes=startedNodes)


def testInstChangeWithMoreReqLat(looper, setup):
    nodes = setup.nodes
    for node in nodes:
        node.checkPerformance()
        assert any(getAllReturnVals(node.monitor,
                                    node.monitor.isMasterReqLatencyTooHigh))

    waitForViewChange(looper, nodes)
