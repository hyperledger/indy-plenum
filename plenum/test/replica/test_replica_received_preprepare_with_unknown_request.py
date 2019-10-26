from plenum.server.consensus.ordering_service import OrderingService
from plenum.server.node import Node
from plenum.server.replica import Replica
from plenum.server.suspicion_codes import Suspicions
from plenum.test.delayers import ppgDelay
from plenum.test.node_request.test_propagate.helper import sum_of_request_propagates
from plenum.test.stasher import delay_rules
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.checkpoints.conftest import chkFreqPatched
from plenum.test.replica.helper import register_pp_ts

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
    params = replica._ordering_service.spylog.getLastParams(OrderingService.process_preprepare)
    pp = params["pre_prepare"]
    sender = params["sender"]
    start_request_propagate_count = replica.node.spylog.count(Node.request_propagates)

    def discard(offendingMsg, reason, logger, cliOutput=False):
        assert offendingMsg == pp
        assert Suspicions.PPR_WITH_ORDERED_REQUEST.reason == reason

    replica.node.discard = discard

    register_pp_ts(replica, pp, sender)
    replica._ordering_service.process_preprepare(pp, sender)

    assert 0 == replica.node.spylog.count(Node.request_propagates) - start_request_propagate_count
    assert (pp, sender, set(pp.reqIdr)) not in replica._ordering_service.prePreparesPendingFinReqs


def test_replica_received_preprepare_with_unknown_request(looper,
                                                          txnPoolNodeSet,
                                                          sdk_pool_handle,
                                                          sdk_wallet_steward,
                                                          chkFreqPatched,
                                                          tconf):
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 1)
    slow_nodes = txnPoolNodeSet[2:]
    nodes_stashers = [n.nodeIbStasher for n in slow_nodes]
    slow_replica_1 = txnPoolNodeSet[2].master_replica
    slow_replica_2 = txnPoolNodeSet[3].master_replica
    with delay_rules(nodes_stashers, ppgDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 1)

    params1 = slow_replica_1._ordering_service.spylog.getLastParams(OrderingService.process_preprepare)
    pp1 = params1["pre_prepare"]
    sender1 = params1["sender"]
    params2 = slow_replica_2._ordering_service.spylog.getLastParams(OrderingService.process_preprepare)
    pp2 = params2["pre_prepare"]
    sender2 = params2["sender"]
    looper.runFor(tconf.PROPAGATE_REQUEST_DELAY)
    assert (pp1, sender1, set(pp1.reqIdr)) not in slow_replica_1._ordering_service.prePreparesPendingFinReqs
    assert (pp2, sender2, set(pp2.reqIdr)) not in slow_replica_2._ordering_service.prePreparesPendingFinReqs
