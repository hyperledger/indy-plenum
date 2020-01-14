from plenum.common.constants import COMMIT, PREPREPARE, PREPARE
from plenum.server.replica_validator_enums import STASH_VIEW_3PC
from plenum.test.delayers import msg_rep_delay, nv_delay
from plenum.test.helper import waitForViewChange, sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change_service.helper import trigger_view_change
from stp_core.loop.eventually import eventually


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
    old_stashed = {inst_id: r.stasher.stash_size(STASH_VIEW_3PC)
                   for inst_id, r in slow_node.replicas.items()}
    with delay_rules([slow_node.nodeIbStasher, ],
                     msg_rep_delay(types_to_delay=[PREPREPARE, PREPARE, COMMIT])):
        with delay_rules([slow_node.nodeIbStasher, ], nv_delay()):
            trigger_view_change(txnPoolNodeSet)
            waitForViewChange(looper, fast_nodes, expectedViewNo=view_no + 1,
                              customTimeout=2 * tconf.NEW_VIEW_TIMEOUT)
            ensureElectionsDone(looper=looper,
                                nodes=fast_nodes,
                                instances_list=range(fast_nodes[0].requiredNumberOfInstances))
            sdk_send_random_and_check(looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_steward,
                                      1)
            assert slow_node.view_change_in_progress
            # 1 - pre-prepare msg
            # (len(txnPoolNodeSet) - 2) - prepare msgs
            # (len(txnPoolNodeSet) - 1) - commit msgs
            stashed_master_messages = 2 * (1 + (len(txnPoolNodeSet) - 2) + (len(txnPoolNodeSet) - 1))
            assert slow_node.master_replica.stasher.stash_size(STASH_VIEW_3PC) == old_stashed[
                0] + stashed_master_messages

        def chk():
            for inst_id, r in slow_node.replicas.items():
                assert r.last_ordered_3pc[1] == 2
                assert r.stasher.stash_size(STASH_VIEW_3PC) == 0

        looper.run(eventually(chk))
        waitNodeDataEquality(looper, slow_node, *fast_nodes)
