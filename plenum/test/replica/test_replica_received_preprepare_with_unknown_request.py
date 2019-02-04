from plenum.server.node import Node
from plenum.server.replica import Replica
from plenum.server.suspicion_codes import Suspicions
from plenum.test.delayers import ppgDelay
from plenum.test.node_request.test_propagate.helper import sum_of_request_propagates
from plenum.test.stasher import delay_rules
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.checkpoints.conftest import chkFreqPatched

logger = getlogger()
CHK_FREQ = 1


def test_replica_received_preprepare_with_ordered_request(looper,
                                                          txnPoolNodeSet,
                                                          sdk_pool_handle,
                                                          sdk_wallet_steward,
                                                          chkFreqPatched):
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 1)

    replica = txnPoolNodeSet[1].master_replica
    params = replica.spylog.getLastParams(Replica.processPrePrepare)
    pp = params["pre_prepare"]
    sender = params["sender"]
    start_request_propagate_count = replica.node.spylog.count(Node.request_propagates)

    def discard(offendingMsg, reason, logger, cliOutput=False):
        assert offendingMsg == pp
        assert Suspicions.PPR_WITH_ORDERED_REQUEST.reason == reason

    replica.node.discard = discard

    replica.processPrePrepare(pp, sender)

    assert 0 == replica.node.spylog.count(Node.request_propagates) - start_request_propagate_count
    assert (pp, sender, set(pp.reqIdr)) not in replica.prePreparesPendingFinReqs


def test_replica_received_preprepare_with_unknown_request(looper,
                                                          txnPoolNodeSet,
                                                          sdk_pool_handle,
                                                          sdk_wallet_steward,
                                                          chkFreqPatched,
                                                          tconf):
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 1)
    replica = txnPoolNodeSet[1].master_replica
    start_request_propagate_count = sum_of_request_propagates(txnPoolNodeSet[1])
    with delay_rules(txnPoolNodeSet[1].nodeIbStasher, ppgDelay(delay=10)):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 1)

    params = replica.spylog.getLastParams(Replica.processPrePrepare)
    pp = params["pre_prepare"]
    sender = params["sender"]
    looper.runFor(tconf.PROPAGATE_REQUEST_DELAY)
    assert (pp, sender, set(pp.reqIdr)) not in replica.prePreparesPendingFinReqs
    assert 1 == sum_of_request_propagates(txnPoolNodeSet[1]) - start_request_propagate_count
