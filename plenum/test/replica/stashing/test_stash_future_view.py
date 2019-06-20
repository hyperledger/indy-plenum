import pytest as pytest

from plenum.common.constants import COMMIT, PREPREPARE, PREPARE
from plenum.common.startable import Mode
from plenum.test.delayers import vcd_delay, msg_rep_delay
from plenum.test.helper import waitForViewChange, sdk_send_random_and_check, assertExp
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
            stashed_master_messages = 2 * (1 + (len(txnPoolNodeSet) - 2) + (len(txnPoolNodeSet) - 1))
            stashed_backup_messages = 2 * (1 + (len(txnPoolNodeSet) - 2) + (len(txnPoolNodeSet) - 1))
            assert slow_node.master_replica.stasher.num_stashed_future_view == old_stashed[0] + stashed_master_messages
            assert all(r.stasher.num_stashed_future_view == old_stashed[inst_id] + stashed_backup_messages
                       for inst_id, r in slow_node.replicas.items() if inst_id != 0)
            assert all(r.last_ordered_3pc == last_ordered[inst_id]
                       for inst_id, r in slow_node.replicas.items())

        def chk():
            for inst_id, r in slow_node.replicas.items():
                if inst_id == 0:
                    assert r.last_ordered_3pc == (view_no + 1, 2)
                else:
                    assert r.last_ordered_3pc == (view_no + 1, 2)
                assert r.stasher.num_stashed_future_view == old_stashed[inst_id]

        looper.run(eventually(chk))
        waitNodeDataEquality(looper, slow_node, *fast_nodes)
