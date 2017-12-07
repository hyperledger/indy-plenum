from stp_core.loop.eventually import eventually

from plenum.test import waits
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.delayers import ppDelay
from plenum.test.test_node import getPrimaryReplica
from plenum.test.helper import sdk_send_random_and_check


def test_stable_checkpoint_when_one_instance_slow(chkFreqPatched, looper, txnPoolNodeSet, sdk_pool_handle,
                                                  sdk_wallet_client, reqs_for_checkpoint):
    delay = 5
    pr = getPrimaryReplica(txnPoolNodeSet, 1)
    slowNode = pr.node
    otherNodes = [n for n in txnPoolNodeSet if n != slowNode]
    for n in otherNodes:
        n.nodeIbStasher.delay(ppDelay(delay, 1))

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, reqs_for_checkpoint)
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)) + delay
    looper.run(eventually(chkChkpoints, txnPoolNodeSet, 1, 0, retryWait=1, timeout=timeout))
