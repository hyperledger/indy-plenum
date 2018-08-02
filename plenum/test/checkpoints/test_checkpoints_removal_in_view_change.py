import pytest
import sys

from plenum.common.constants import CHECKPOINT, COMMIT, CONSISTENCY_PROOF
from plenum.common.messages.node_messages import Checkpoint, CheckpointState
from plenum.test.delayers import cDelay, chk_delay, cpDelay
from plenum.test.helper import sdk_send_random_requests, \
    sdk_get_and_check_replies, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules
from plenum.test.test_node import getNonPrimaryReplicas, getAllReplicas, \
    getPrimaryReplica, ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change_complete
from stp_core.loop.eventually import eventually

CHK_FREQ = 2


def delay_msg(nodes, delay_function):
    for n in nodes:
        n.nodeIbStasher.delay(delay_function(sys.maxsize))


def reset_delay(nodes, message):
    for n in nodes:
        n.nodeIbStasher.reset_delays_and_process_delayeds(message)


def test_checkpoints_removed_in_view_change(chkFreqPatched,
                                            txnPoolNodeSet,
                                            looper,
                                            sdk_pool_handle,
                                            sdk_wallet_client):
    primary = txnPoolNodeSet[0]
    slow_nodes = txnPoolNodeSet[:1]
    correct_nodes = txnPoolNodeSet[1:3]
    fast_nodes = txnPoolNodeSet[3:]
    # sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
    #                           sdk_wallet_client, CHK_FREQ)
    # for node in txnPoolNodeSet:
    #     node.view_changer.on_master_degradation()
    delay_msg(slow_nodes, chk_delay)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, CHK_FREQ)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    delay_msg(slow_nodes, cDelay)
    delay_msg(correct_nodes, cDelay)

    requests1 = sdk_send_random_requests(looper, sdk_pool_handle,
                                         sdk_wallet_client, 1)
    looper.run(eventually(last_prepared_certificate,
                          slow_nodes,
                          (0, CHK_FREQ + 1)))
    looper.run(eventually(last_ordered_check,
                          fast_nodes,
                          (0, CHK_FREQ + 1)))
    looper.run(eventually(checkpoint_finish,
                          correct_nodes,
                          1, CHK_FREQ))
    delay_msg(txnPoolNodeSet, cpDelay)
    # trigger view change on all nodes
    for node in txnPoolNodeSet:
        node.view_changer.on_master_degradation()
    looper.run(eventually(check_view_no,
                          slow_nodes,
                          1))
    # for node in txnPoolNodeSet:
    #     node.viewNo += 1
    #     node.view_changer.view_change_in_progress = True
    reset_delay(slow_nodes, CHECKPOINT)
    looper.run(eventually(checkpoint_finish,
                          slow_nodes,
                          1, CHK_FREQ))

    reset_delay(slow_nodes, COMMIT)
    reset_delay(correct_nodes, COMMIT)
    sdk_get_and_check_replies(looper, requests1)
    reset_delay(txnPoolNodeSet, CONSISTENCY_PROOF)
    ensureElectionsDone(looper, txnPoolNodeSet)

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, CHK_FREQ)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    # assert primary.master_last_ordered_3PC[1] == 2*CHK_FREQ + 1


def last_prepared_certificate(nodes, num):
    for n in nodes:
        assert n.master_replica.last_prepared_certificate_in_view() == num


def checkpoint_finish(nodes, start_pp_seq_no, end_pp_seq_no):
    for n in nodes:
        # assert n.master_replica.firstCheckPoint is not None
        # assert n.master_replica.firstCheckPoint[0] == (start_pp_seq_no,
        #                                                end_pp_seq_no)
        for seq_no in range(start_pp_seq_no, end_pp_seq_no):
            assert not (0, seq_no) in n.master_replica.sentPrePrepares


def last_ordered_check(nodes, last_ordered):
    for n in nodes:
        assert n.master_last_ordered_3PC == last_ordered


def check_view_no(nodes, view_no):
    for n in nodes:
        assert n.viewNo == view_no