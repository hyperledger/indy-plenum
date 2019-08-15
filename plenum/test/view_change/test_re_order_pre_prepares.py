import pytest

from plenum.common.messages.internal_messages import ViewChangeStarted
from plenum.test.delayers import cDelay, pDelay
from plenum.test.helper import sdk_send_random_and_check, assert_eq
from plenum.test.stasher import delay_rules_without_processing
from stp_core.loop.eventually import eventually


@pytest.mark.skip()
def test_re_order_pre_prepares(looper, txnPoolNodeSet,
                               sdk_wallet_client, sdk_pool_handle):
    # 1. drop Prepares and Commits on 4thNode
    # Order a couple of requests on Nodes 1-3
    lagging_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]
    with delay_rules_without_processing(lagging_node.nodeIbStasher, cDelay(), pDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 2)
        assert all(n.master_last_ordered_3PC == (0, 2) for n in other_nodes)

    # 2. simulate view change start so that
    # all PrePrepares/Prepares/Commits are cleared
    for n in txnPoolNodeSet:
        n.internal_bus.send(ViewChangeStarted(view_no=1))
        assert not n.master_replica.prePrepares
        assert not n.master_replica.prepares
        assert not n.master_replica.commits
        assert n.master_replica.old_view_preprepares

    # 3. Re-order the same PrePrepare
    new_master = txnPoolNodeSet[1]
    new_other_nodes = [txnPoolNodeSet[0]] + txnPoolNodeSet[2:]
    assert lagging_node.master_last_ordered_3PC == (0, 0)
    for pp in new_master.master_replica._ordering_service.old_view_preprepares:
        for n in new_other_nodes:
            n.master_replica._ordering_service.process_preprepare(pp)

    # 4. Make sure that the nodes 1-3 (that already ordered the requests) sent Prepares and Commits so that
    # the request was eventually ordered on Node4 as well
    looper.run(eventually(lambda: assert_eq(lagging_node.master_last_ordered_3PC, (0, 2))))
