import sys

from plenum.common.constants import COMMIT, PREPREPARE, PREPARE, LEDGER_STATUS
from plenum.common.startable import Mode
from plenum.server.replica_validator_enums import STASH_CATCH_UP, STASH_VIEW_3PC
from plenum.test.delayers import msg_rep_delay, cDelay, cr_delay
from plenum.test.helper import sdk_send_random_and_check, assertExp, sdk_send_random_request, \
    sdk_get_and_check_replies, get_pp_seq_no
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.eventually import eventually


def test_unstash_three_phase_msg_after_catchup(txnPoolNodeSet, looper, tconf,
                                               sdk_pool_handle,
                                               sdk_wallet_steward):
    """
    1. Delay Commit on Node4
    2. Order 1 req
    3. Delay Commit on all nodes
    4. Order 1 req
    5. Delay CatchupRep on Node4
    6. Delay Ledger Status and ViewChangeDones on Nodes1-3
    7. Start View change on all nodes
    8. Wait until Node4 got 3 stashed CatchupReps
    9. Reset delaying of Commits on all Nodes
    10. Reset Ledger Status on Nodes1-3
    11. Check that 3 nodes finished VC while Node4 is syncing and not finished
    12. Reset CatchupRep on Node4
    13. Check that Node4 finished VC, and there was just 1 round of catch-up
    """
    slow_node = txnPoolNodeSet[-1]
    fast_nodes = txnPoolNodeSet[:-1]
    view_no = txnPoolNodeSet[0].viewNo
    old_stashed = slow_node.master_replica.stasher.stash_size(STASH_VIEW_3PC)
    last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    batches_count = last_ordered[1]

    with delay_rules([n.nodeIbStasher for n in txnPoolNodeSet],
                     msg_rep_delay(types_to_delay=[PREPREPARE, PREPARE, COMMIT])):

        # Delay Commit messages for slow_node.
        slow_node.nodeIbStasher.delay(cDelay(sys.maxsize))
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_steward, 1)
        batches_count += 1

        # Delay Commit messages for fast_nodes.
        for n in fast_nodes:
            n.nodeIbStasher.delay(cDelay(sys.maxsize))

        request2 = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_steward)
        batches_count += 1

        def check_commits(commit_key):
            for n in fast_nodes:
                for r in n.replicas.values():
                    assert commit_key in r._ordering_service.commits
                    assert len(r._ordering_service.commits[commit_key].voters) == 1

        looper.run(eventually(check_commits,
                              (view_no, last_ordered[1] + batches_count)))

        # Delay CatchupRep messages for the slow_node.
        with delay_rules([slow_node.nodeIbStasher], cr_delay()):
            with delay_rules([n.nodeIbStasher for n in fast_nodes],
                             msg_rep_delay(types_to_delay=[LEDGER_STATUS])):

                for n in txnPoolNodeSet:
                    n.start_catchup()
                looper.run(eventually(lambda: assertExp(slow_node.mode == Mode.discovering)))

                # Reset delay Commit messages for all nodes.
                for n in txnPoolNodeSet:
                    n.nodeIbStasher.reset_delays_and_process_delayeds(COMMIT)

                assert slow_node.mode == Mode.discovering
                looper.run(eventually(_check_nodes_stashed,
                                      fast_nodes,
                                      old_stashed,
                                      len(txnPoolNodeSet) - 1))
                looper.run(eventually(_check_nodes_stashed,
                                      [slow_node],
                                      old_stashed,
                                      (len(txnPoolNodeSet) - 1) * 2))

        sdk_get_and_check_replies(looper, [request2])
        _check_nodes_stashed(fast_nodes, old_stashed, 0)
        assert get_pp_seq_no(txnPoolNodeSet) == batches_count

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)


def _check_nodes_stashed(nodes, old_stashed, new_stashed):
    for n in nodes:
        assert n.master_replica.stasher.stash_size(STASH_CATCH_UP) == old_stashed + new_stashed
