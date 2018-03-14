from plenum.common.util import getMaxFailures
from plenum.test.helper import checkViewNoForNodes, sdk_send_random_and_check
from plenum.test.delayers import ppDelay
from plenum.test.test_node import TestReplica, getNonPrimaryReplicas

nodeCount = 7
F = getMaxFailures(nodeCount)


# noinspection PyIncorrectDocstring
def test_view_not_changed(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client):
    """
    Test that a view change is not done when the performance of master does
    not go down
    """
    """
    Send multiple requests to the client and delay some requests by all
    backup instances to ensure master instance
    is always faster than backup instances and there is no view change
    """

    # Delay PRE-PREPARE for all backup protocol instances so master performs
    # better
    for i in range(1, F + 1):
        nonPrimReps = getNonPrimaryReplicas(txnPoolNodeSet, i)
        # type: Iterable[TestReplica]
        for r in nonPrimReps:
            r.node.nodeIbStasher.delay(ppDelay(10, i))

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)

    checkViewNoForNodes(txnPoolNodeSet, expectedViewNo=0)
