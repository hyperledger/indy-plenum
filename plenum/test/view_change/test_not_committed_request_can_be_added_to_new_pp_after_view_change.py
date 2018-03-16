import types

from stp_core.common.log import getlogger
from stp_core.loop.eventually import eventually
from plenum.server.tpcrequest import TPCRequest

from plenum.test import waits
from plenum.test.delayers import pDelay
from plenum.test.helper import sdk_signed_random_requests, \
    sdk_send_signed_requests
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.test_node import ensureElectionsDone


from plenum.test.pool_transactions.conftest import looper

logger = getlogger()

def test_not_committed_request_can_be_added_to_new_pp_after_view_change(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, tconf):

    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(pDelay(300))

    one_req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)
    sdk_send_signed_requests(sdk_pool_handle, one_req)
    looper.runFor(waits.expectedPropagateTime(len(txnPoolNodeSet)) +
                  waits.expectedPrePrepareTime(len(txnPoolNodeSet)))

    passed = False

    def check_pp(self, pp):
        nonlocal passed
        for tpc_key in pp.reqIdr:
            rbftrequest = self.requests.get(tpc_key)
            assert rbftrequest
            rbftrequest.on_tpcevent(self.node.instances.masterId, TPCRequest.Accept((pp.viewNo, pp.ppSeqNo)))
        passed = True

    def check_passed():
        assert passed

    for node in txnPoolNodeSet:
      node.replicas._master_replica.sendPrePrepare = types.MethodType(
            check_pp, node.replicas._master_replica)

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    looper.run(eventually(
        check_passed, retryWait=1, timeout=(tconf.Max3PCBatchWait + 1)))
