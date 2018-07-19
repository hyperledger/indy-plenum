import pytest

from plenum.common.messages.node_messages import PrePrepare
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.helper import waitForViewChange
from plenum.test.spy_helpers import getAllReturnVals
from plenum.test.test_node import getPrimaryReplica
from stp_core.common.util import adict

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
def tconf(tconf):
    oldMax3PCBatchSize = tconf.Max3PCBatchSize
    oldMax3PCBatchWait = tconf.Max3PCBatchWait
    tconf.Max3PCBatchSize = 5
    tconf.Max3PCBatchWait = 1000
    yield tconf

    tconf.Max3PCBatchSize = oldMax3PCBatchSize
    tconf.Max3PCBatchWait = oldMax3PCBatchWait



@pytest.fixture()
def setup(looper, tconf, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle):
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 5)
    P = getPrimaryReplica(txnPoolNodeSet)

    # set LAMBDA smaller than the production config to make the test faster
    testLambda = 10
    delay_by = 2 * testLambda

    old_view_nos = set([n.viewNo for n in txnPoolNodeSet])
    assert len(old_view_nos) == 1
    old_view_no = old_view_nos.pop()

    for node in txnPoolNodeSet:
        # Make `Delta` small enough so throughput check passes.
        node.monitor.Delta = .001
        node.monitor.Lambda = testLambda
        for r in node.replicas:
            r.config.ACCEPTABLE_DEVIATION_PREPREPARE_SECS += delay_by

    # make P (primary replica on master) faulty, i.e., slow to send
    # PRE-PREPARE the next
    def specificPrePrepare(msg):
        if isinstance(msg, PrePrepare):
            return delay_by  # just more that LAMBDA

    P.outBoxTestStasher.delay(specificPrePrepare)
    # TODO select or create a timeout for this case in 'waits'
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_client, 5,
                              customTimeoutPerReq=tconf.TestRunningTimeLimitSec)

    return adict(nodes=txnPoolNodeSet, old_view_no=old_view_no)


@pytest.mark.skip(reason="Turn off isMasterReqLatencyTooHigh for current view_change protocol")
def testInstChangeWithMoreReqLat(looper, setup):
    # TODO for now, view_change procedure can take more that 15 minutes
    # (5 minutes for catchup and 10 minutes for primary's answer).
    # Therefore, view_change triggering by max latency now is not indicative.
    nodes = setup.nodes
    old_view_no = setup.old_view_no
    for node in nodes:
        node.checkPerformance()
        assert any(getAllReturnVals(node.monitor,
                                    node.monitor.isMasterReqLatencyTooHigh))

    waitForViewChange(looper, nodes, expectedViewNo=old_view_no + 1)
