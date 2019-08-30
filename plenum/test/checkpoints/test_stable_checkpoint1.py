from plenum.test.checkpoints.helper import check_for_nodes, check_stable_checkpoint, check_num_unstable_checkpoints
from stp_core.loop.eventually import eventually

from plenum.test import waits
from plenum.test.delayers import ppDelay
from plenum.test.test_node import getPrimaryReplica
from plenum.test.helper import sdk_send_random_and_check


def test_stable_checkpoint_when_one_instance_slow(chkFreqPatched, tconf, looper, txnPoolNodeSet, sdk_pool_handle,
                                                  sdk_wallet_client, reqs_for_checkpoint):
    delay = 5
    pr = getPrimaryReplica(txnPoolNodeSet, 1)
    slowNode = pr.node
    otherNodes = [n for n in txnPoolNodeSet if n != slowNode]
    for n in otherNodes:
        n.nodeIbStasher.delay(ppDelay(delay, 1))

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, reqs_for_checkpoint)
    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet)) + delay
    next_checkpoint = tconf.CHK_FREQ
    looper.run(eventually(check_for_nodes, txnPoolNodeSet, check_stable_checkpoint, next_checkpoint,
                          retryWait=1, timeout=timeout))
    check_for_nodes(txnPoolNodeSet, check_num_unstable_checkpoints, 0)
