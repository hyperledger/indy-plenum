import sys

import pytest as pytest

from plenum.common.constants import COMMIT, PREPREPARE, PREPARE, LEDGER_STATUS
from plenum.common.startable import Mode
from plenum.test.delayers import vcd_delay, msg_rep_delay, cDelay, cr_delay, lsDelay
from plenum.test.helper import waitForViewChange, sdk_send_random_and_check, assertExp, sdk_send_random_request, \
    sdk_get_and_check_replies
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.eventually import eventually

@pytest.fixture(scope="module")
def tconf(tconf):
    tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE = 20
    return tconf


def test_process_three_phase_msg_and_stashed_future_view(txnPoolNodeSet, looper, tconf,
                                                         sdk_pool_handle,
                                                         sdk_wallet_steward):
    """
    1. Delay ViewChangeDone messages for the slow_node.
    2. Start view change on all nodes.
    3. Order a new request.
    4. Check that slow_node could not order this request and stashed all 3pc messages
    and other nodes ordered.
    6. Reset delays.
    7. Check that the last request is ordered on the slow_node and stashed messages were removed.
    """
    slow_node = txnPoolNodeSet[-1]
    fast_nodes = txnPoolNodeSet[:-1]
    view_no = slow_node.viewNo
    old_stashed = {inst_id: r.stasher.num_stashed_future_view
                   for inst_id, r in slow_node.replicas.items()}
    last_ordered = {inst_id: r.last_ordered_3pc
                    for inst_id, r in slow_node.replicas.items()}
    with delay_rules([slow_node.nodeIbStasher, ],
                     msg_rep_delay(types_to_delay=[PREPREPARE, PREPARE, COMMIT])):
        with delay_rules([slow_node.nodeIbStasher, ], vcd_delay()):
            for n in txnPoolNodeSet:
                n.view_changer.on_master_degradation()
            waitForViewChange(looper, fast_nodes, expectedViewNo=view_no + 1,
                              customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
            ensureElectionsDone(looper=looper,
                                nodes=fast_nodes,
                                instances_list=range(fast_nodes[0].requiredNumberOfInstances))
            looper.run(eventually(lambda: assertExp(slow_node.mode == Mode.synced)))
            sdk_send_random_and_check(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_steward,
                                      1)
            assert slow_node.view_change_in_progress
            # 1 - pre-prepare msg
            # (len(txnPoolNodeSet) - 2) - prepare msgs
            # (len(txnPoolNodeSet) - 1) - commit msgs
            stashed_messages = 1 + (len(txnPoolNodeSet) - 2) + (len(txnPoolNodeSet) - 1)
            assert all(r.stasher.num_stashed_future_view == old_stashed[inst_id] + stashed_messages
                       for inst_id, r in slow_node.replicas.items())
            assert all(r.last_ordered_3pc == last_ordered[inst_id]
                       for inst_id, r in slow_node.replicas.items())

        def chk():
            for inst_id, r in slow_node.replicas.items():
                assert r.last_ordered_3pc == (view_no + 1, 1)  # ordered a new batch in a new view
                assert r.stasher.num_stashed_future_view == old_stashed[inst_id]

        looper.run(eventually(chk))
        waitNodeDataEquality(looper, slow_node, *fast_nodes)


def test_unstash_three_phase_msg_after_catchup_in_view_change(txnPoolNodeSet, looper, tconf,
                                                         sdk_pool_handle,
                                                         sdk_wallet_steward):
    """
    1. Delay Commit messages for all nodes.
    2. Order a new request.
    3. Delay CatchupRep messages for all nodes.
    4. Check that nodes could not order this request and stashed preprepare and prepares.
    5. Reset delay for Commit.
    6. Check that nodes could not order this request and stashed commits.
    7. Reset delays for CatchupRep.
    8. Check that the request is ordered on all nodes and stashed messages were removed,
    view changed is finished after just 1 round of catch-up because last_prepared_cert is reached.
    """
    slow_node = txnPoolNodeSet[-1]
    fast_nodes = txnPoolNodeSet[:-1]
    view_no = txnPoolNodeSet[0].viewNo
    old_stashed = slow_node.master_replica.stasher.num_stashed_future_view
    last_ordered = {inst_id: r.last_ordered_3pc
                    for inst_id, r in txnPoolNodeSet[0].replicas.items()}

    with delay_rules([n.nodeIbStasher for n in txnPoolNodeSet],
                     msg_rep_delay(types_to_delay=[PREPREPARE, PREPARE, COMMIT])):

        # Delay Commit messages for slow_node.
        slow_node.nodeIbStasher.delay(cDelay(sys.maxsize))
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_steward, 1)

        # 1 - pre-prepare msg
        # (len(txnPoolNodeSet) - 2) - prepare msgs
        stashed_messages = 1 + (len(txnPoolNodeSet) - 2)

        # Delay Commit messages for fast_nodes.
        for n in fast_nodes:
            n.nodeIbStasher.delay(cDelay(sys.maxsize))

        request2 = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_steward)

        def check_commits(commit_key):
            for n in txnPoolNodeSet:
                assert all(len(r.commits[commit_key].voters) == 1 for r in n.replicas.values())

        looper.run(eventually(check_commits,
                              (view_no, last_ordered[0][1] + 2)))

        print(txnPoolNodeSet[-1].master_replica.prePrepares)

        # Delay MessageRep messages for the slow_node.
        with delay_rules([slow_node.nodeIbStasher], cr_delay()):
            with delay_rules([n.nodeIbStasher for n in fast_nodes],
                             msg_rep_delay(types_to_delay=[LEDGER_STATUS])):
                for n in txnPoolNodeSet:
                    print(n.master_replica.last_prepared_before_view_change)
                    n.view_changer.on_master_degradation()
                looper.run(eventually(lambda: assertExp(slow_node.mode == Mode.syncing)))
                # for n in fast_nodes:
                #     looper.run(eventually(lambda: assertExp(n.mode == Mode.syncing)))
                print(txnPoolNodeSet[-1].master_replica.prePrepares)

                # Reset delay Commit messages for all nodes.
                for n in txnPoolNodeSet:
                    n.nodeIbStasher.reset_delays_and_process_delayeds(COMMIT)

                assert slow_node.view_change_in_progress
                print("ViewNo: {}".format(fast_nodes[0].viewNo))
                assert fast_nodes[0].view_change_in_progress
                assert slow_node.mode == Mode.syncing
                looper.run(eventually(_check_nodes_stashed, [slow_node], old_stashed, (len(txnPoolNodeSet) - 1) * 2))

            # waitForViewChange(looper, fast_nodes, expectedViewNo=view_no + 1,
            #                   customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
            # ensureElectionsDone(looper=looper,
            #                     nodes=fast_nodes,
            #                     instances_list=range(fast_nodes[0].requiredNumberOfInstances))
        sdk_get_and_check_replies(looper, [request2])
        waitForViewChange(looper, [slow_node], expectedViewNo=view_no + 1,
                          customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
        ensureElectionsDone(looper=looper,
                            nodes=txnPoolNodeSet)
        _check_nodes_stashed(txnPoolNodeSet, old_stashed, 0)
        print(slow_node.master_replica.last_ordered_3pc)
        assert all(r.last_ordered_3pc == (last_ordered[inst_id][0],
                                      last_ordered[inst_id][1] + 2)
                   for inst_id, r in slow_node.replicas.items())




def _check_nodes_stashed(nodes, old_stashed, new_stashed):
    for n in nodes:
        assert n.master_replica.stasher.num_stashed_catchup == old_stashed + new_stashed
