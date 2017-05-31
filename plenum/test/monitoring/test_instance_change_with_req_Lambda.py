from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.types import PrePrepare, f
from plenum.common.util import adict
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
    # Get the master replica of the master protocol instance
    P = getPrimaryReplica(startedNodes)

    # Make `Delta` small enough so throughput check passes.
    for node in startedNodes:
        node.monitor.Delta = .001

    # set LAMBDA not so huge like it set in the production config
    testLambda = 15
    for node in startedNodes:
        node.monitor.Lambda = testLambda

    slowed_request = False
    slow_by = testLambda + 5

    # make P (primary replica on master) faulty, i.e., slow to send
    # PRE-PREPARE for a specific client request only
    def specificPrePrepare(msg):
        nonlocal slowed_request
        if isinstance(msg, PrePrepare) and slowed_request is False:
            slowed_request = True
            return slow_by  # just more that LAMBDA

    P.outBoxTestStasher.delay(specificPrePrepare)
    # TODO select or create a timeout for this case in 'waits'
    sendReqsToNodesAndVerifySuffReplies(looper,
                                        wallet1,
                                        client1,
                                        numReqs=5,
                                        add_delay_to_timeout=slow_by)

    return adict(nodes=startedNodes)


def testInstChangeWithMoreReqLat(looper, setup):
    nodes = setup.nodes
    for node in nodes:
        node.checkPerformance()
        assert any(getAllReturnVals(node.monitor,
                                    node.monitor.isMasterReqLatencyTooHigh))

    waitForViewChange(looper, nodes)
