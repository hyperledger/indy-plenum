import pytest

from plenum.common.messages.node_messages import PrePrepare
from stp_core.common.util import adict
from plenum.test.helper import waitForViewChange
from plenum.test.test_node import getPrimaryReplica
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.pool_transactions.conftest import looper

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


@pytest.fixture()
def setup(looper, tconf, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                          sdk_pool_handle, sdk_wallet_client, 5)
    P = getPrimaryReplica(txnPoolNodeSet)

    # set LAMBDA smaller than the production config to make the test faster
    testLambda = 30
    delay_by = testLambda + 5

    for node in txnPoolNodeSet:
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
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                          sdk_pool_handle, sdk_wallet_client, 5,
                          customTimeoutPerReq=tconf.TestRunningTimeLimitSec)

    return adict(nodes=txnPoolNodeSet)


def testInstChangeWithMoreReqLat(looper, setup):
    nodes = setup.nodes
    for node in nodes:
        node.checkPerformance()
        assert any(getAllReturnVals(node.monitor,
                                    node.monitor.isMasterReqLatencyTooHigh))

    waitForViewChange(looper, nodes)
