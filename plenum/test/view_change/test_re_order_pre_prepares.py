import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.internal_messages import ViewChangeStarted, NewViewCheckpointsApplied
from plenum.server.consensus.utils import preprepare_to_batch_id
from plenum.test.delayers import cDelay, pDelay
from plenum.test.helper import sdk_send_random_and_check, max_3pc_batch_limits
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules_without_processing


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf


def test_re_order_pre_prepares(looper, txnPoolNodeSet,
                               sdk_wallet_client, sdk_pool_handle):
    # 1. drop Prepares and Commits on 4thNode
    # Order a couple of requests on Nodes 1-3
    lagging_node = txnPoolNodeSet[-1]
    other_nodes = txnPoolNodeSet[:-1]
    with delay_rules_without_processing(lagging_node.nodeIbStasher, cDelay(), pDelay()):
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 3)
        assert all(n.master_last_ordered_3PC == (0, 3) for n in other_nodes)

    # 2. simulate view change start so that
    # all PrePrepares/Prepares/Commits are cleared
    # and uncommitted txns are reverted
    for n in txnPoolNodeSet:
        n.replicas.send_to_internal_bus(ViewChangeStarted(view_no=1))
        master_ordering_service = n.master_replica._ordering_service
        assert not master_ordering_service.prePrepares
        assert not master_ordering_service.prepares
        assert not master_ordering_service.commits
        assert master_ordering_service.old_view_preprepares
        ledger = n.db_manager.ledgers[DOMAIN_LEDGER_ID]
        state = n.db_manager.states[DOMAIN_LEDGER_ID]
        assert len(ledger.uncommittedTxns) == 0
        assert ledger.uncommitted_root_hash == ledger.tree.root_hash
        assert state.committedHead == state.head

    # 3. Simulate View Change finish to re-order the same PrePrepare
    assert lagging_node.master_last_ordered_3PC == (0, 0)
    new_master = txnPoolNodeSet[1]
    batches = sorted([preprepare_to_batch_id(pp) for _, pp in
                      new_master.master_replica._ordering_service.old_view_preprepares.items()])
    new_view_msg = NewViewCheckpointsApplied(view_no=0,
                                             view_changes=[],
                                             checkpoint=None,
                                             batches=batches)
    for n in txnPoolNodeSet:
        n.master_replica._consensus_data.prev_view_prepare_cert = batches[-1].pp_seq_no
        n.master_replica._ordering_service._bus.send(new_view_msg)

    # 4. Make sure that the nodes 1-3 (that already ordered the requests) sent Prepares and Commits so that
    # the request was eventually ordered on Node4 as well
    waitNodeDataEquality(looper, lagging_node, *other_nodes)
    assert lagging_node.master_last_ordered_3PC == (0, 4)

    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)

