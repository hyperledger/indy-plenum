import pytest

from plenum.common.constants import AUDIT_LEDGER_ID
from plenum.common.messages.node_messages import CatchupReq
from plenum.test.delayers import cDelay, pDelay, cqDelay
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.stasher import delay_rules_without_processing
from stp_core.loop.eventually import eventually

nodeCount = 7


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldSize = tconf.Max3PCBatchSize
    tconf.Max3PCBatchSize = 1

    yield tconf
    tconf.Max3PCBatchSize = oldSize


def test_no_catchup_req_with_absent_req(looper,
                                        txnPoolNodeSet,
                                        sdk_pool_handle,
                                        sdk_wallet_client):
    def check_cathup_req_rec(stasher):
        for d in stasher.delayeds:
            msg = d.item[0]
            if isinstance(msg, CatchupReq):
                return
        assert False

    lagged_node_1 = txnPoolNodeSet[-2]
    lagged_node_2 = txnPoolNodeSet[-1]
    with delay_rules_without_processing(lagged_node_1.nodeIbStasher, cDelay(), pDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 2)

        with delay_rules_without_processing(lagged_node_2.nodeIbStasher, cDelay(), pDelay(), cqDelay()):
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 10)
            lagged_node_1.start_catchup()
            looper.run(eventually(check_cathup_req_rec, lagged_node_2.nodeIbStasher, timeout=10))
            audit_req_size = lagged_node_2.auditLedger.size
            successes = []
            for delayed in lagged_node_2.nodeIbStasher.delayeds:
                msg = delayed.item[0]
                if isinstance(msg, CatchupReq) and msg.ledgerId == AUDIT_LEDGER_ID:
                    assert msg.seqNoEnd <= audit_req_size
                    successes.append(True)
            assert len(successes) > 0

