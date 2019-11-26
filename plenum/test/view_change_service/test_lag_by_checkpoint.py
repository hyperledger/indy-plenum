import pytest

from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_send_random_and_check, checkViewNoForNodes, assertExp, get_pp_seq_no
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change_service.conftest import CHK_FREQ
from plenum.test.view_change_service.helper import trigger_view_change
from stp_core.loop.eventually import eventually


def test_lag_less_then_catchup(looper,
                               txnPoolNodeSet,
                               sdk_pool_handle,
                               sdk_wallet_client):
    delayed_node = txnPoolNodeSet[-1]
    other_nodes = list(set(txnPoolNodeSet) - {delayed_node})
    checkViewNoForNodes(txnPoolNodeSet)
    last_ordered_before = delayed_node.master_replica.last_ordered_3pc
    with delay_rules_without_processing(delayed_node.nodeIbStasher, cDelay()):
        # Send txns for stable checkpoint
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, CHK_FREQ)
        # Check, that all of not slowed nodes has a stable checkpoint
        for n in other_nodes:
            assert n.master_replica._consensus_data.stable_checkpoint == CHK_FREQ

        # Send another txn. This txn will be reordered after view_change
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
        trigger_view_change(txnPoolNodeSet)
        ensureElectionsDone(looper, txnPoolNodeSet)

        assert delayed_node.master_replica.last_ordered_3pc == last_ordered_before

    # Send txns for stabilize checkpoint on other nodes
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, CHK_FREQ - 1)

    pool_pp_seq_no = get_pp_seq_no(other_nodes)
    looper.run(eventually(lambda: assertExp(delayed_node.master_replica.last_ordered_3pc[1] == pool_pp_seq_no)))
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
