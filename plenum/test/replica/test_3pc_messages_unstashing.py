from plenum.common.constants import COMMIT, PREPREPARE, PREPARE
from plenum.common.startable import Mode
from plenum.test.delayers import vcd_delay, chk_delay, msg_rep_delay
from plenum.test.helper import waitForViewChange, sdk_send_random_and_check, assertExp, \
    sdk_send_batches_of_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.eventually import eventually
from plenum.test.checkpoints.conftest import chkFreqPatched

CHK_FREQ = 2
LOG_SIZE = CHK_FREQ


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
    7. Check that the last request is ordered on the slow_node, checkpoint is finalized
    and stashed messages were removed.
    """
    slow_node = txnPoolNodeSet[-1]
    fast_nodes = txnPoolNodeSet[:-1]
    view_no = slow_node.viewNo
    old_stashed = {inst_id: r.stasher.num_stashed_future_view
                   for inst_id, r in slow_node.replicas.items()}
    last_ordered = {inst_id: r.last_ordered_3pc
                    for inst_id, r in slow_node.replicas.items()}
    with delay_rules([slow_node.nodeIbStasher, ], msg_rep_delay(types_to_delay=[PREPREPARE, PREPARE, COMMIT])):
        with delay_rules([slow_node.nodeIbStasher, ], vcd_delay()):
            for n in txnPoolNodeSet:
                n.view_changer.on_master_degradation()
            waitForViewChange(looper, fast_nodes, expectedViewNo=view_no + 1,
                              customTimeout=2 * tconf.VIEW_CHANGE_TIMEOUT)
            ensureElectionsDone(looper=looper,
                                nodes=fast_nodes,
                                instances_list=fast_nodes[0].replicas.keys())
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


def test_process_three_phase_msg_and_stashed_for_next_checkpoint(txnPoolNodeSet,
                                                                 looper,
                                                                 sdk_pool_handle,
                                                                 sdk_wallet_client,
                                                                 chkFreqPatched):
    """
    1. Delay checkpoints processing on the slow_node. That is checkpoint on this node
    can't be finalized.
    2. Order requests for finalize checkpoints.
    3. Check that a checkpoint is finalized on all nodes exclude the slow_node.
    4. Order a new request.
    5. Check that slow_node could not order this request and stashed all 3pc messages.
    6. Reset delays.
    7. Check that the last request is ordered on the slow_node, checkpoint is finalized
    and stashed messages were removed.
    """

    for n in txnPoolNodeSet:
        for r in n.replicas.values():
            r.update_watermark_from_3pc()

    slow_node = txnPoolNodeSet[-1]
    fast_nodes = txnPoolNodeSet[:-1]

    old_stashed = {inst_id: r.stasher.num_stashed_watermarks
                   for inst_id, r in slow_node.replicas.items()}
    last_ordered = {inst_id: r.last_ordered_3pc
                    for inst_id, r in slow_node.replicas.items()}

    with delay_rules([slow_node.nodeIbStasher, ], chk_delay()):
        sdk_send_batches_of_random_and_check(looper,
                                             txnPoolNodeSet,
                                             sdk_pool_handle,
                                             sdk_wallet_client,
                                             num_reqs=1 * CHK_FREQ,
                                             num_batches=CHK_FREQ)
        ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
        looper.run(eventually(_check_checkpoint_finalize,
                              fast_nodes,
                              1, CHK_FREQ))
        sdk_send_random_and_check(looper,
                                  txnPoolNodeSet,
                                  sdk_pool_handle,
                                  sdk_wallet_client,
                                  1)
        # 1 - pre-prepare msg
        # (len(txnPoolNodeSet) - 2) - prepare msgs
        # (len(txnPoolNodeSet) - 1) - commit msgs
        stashed_messages = 1 + (len(txnPoolNodeSet) - 2) + (len(txnPoolNodeSet) - 1)
        assert all(r.stasher.num_stashed_watermarks == old_stashed[inst_id] + stashed_messages
                   for inst_id, r in slow_node.replicas.items())

        _check_batches_ordered(slow_node, last_ordered, CHK_FREQ)
        for n in fast_nodes:
            _check_batches_ordered(n, last_ordered, CHK_FREQ + 1)

    looper.run(eventually(_check_checkpoint_finalize,
                          [slow_node, ],
                          1, CHK_FREQ))
    looper.run(eventually(_check_batches_ordered,
                          slow_node, last_ordered, CHK_FREQ + 1))
    assert all(r.stasher.num_stashed_watermarks == old_stashed[inst_id]
               for inst_id, r in slow_node.replicas.items())


def _check_checkpoint_finalize(nodes, start_pp_seq_no, end_pp_seq_no):
    for n in nodes:
        checkpoint = n.master_replica.checkpoints[(start_pp_seq_no, end_pp_seq_no)]
        assert checkpoint.isStable


def _check_batches_ordered(node, last_ordered, num_batches_ordered):
    assert all(r.last_ordered_3pc == (last_ordered[inst_id][0],
                                      last_ordered[inst_id][1] + num_batches_ordered)
               for inst_id, r in node.replicas.items())
