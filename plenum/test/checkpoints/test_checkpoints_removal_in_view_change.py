from numbers import Rational

import math
import pytest
import sys

from plenum.common.constants import CHECKPOINT, COMMIT
from plenum.test.delayers import cDelay, chk_delay, lsDelay, vcd_delay
from plenum.test.helper import sdk_send_random_requests, \
    sdk_get_and_check_replies, sdk_send_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

CHK_FREQ = 2


@pytest.mark.skip('TODO: Make this test pass')
def test_checkpoints_removed_in_view_change(chkFreqPatched,
                                            txnPoolNodeSet,
                                            looper,
                                            sdk_pool_handle,
                                            sdk_wallet_client):
    '''
    Check that checkpoint finalize in view change before catchup doesn't clean
    necessary data from requests and 3pc queues.
    '''
    slow_nodes = txnPoolNodeSet[:3]
    fast_nodes = txnPoolNodeSet[3:]
    # delay checkpoints processing for slow_nodes
    delay_msg(slow_nodes, chk_delay)
    # send txns for finalizing current checkpoint
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, CHK_FREQ)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    # delay commits processing for slow_nodes
    delay_msg(slow_nodes, cDelay)

    requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                        sdk_wallet_client, 1)
    # check that slow nodes have prepared certificate with new txn
    looper.run(eventually(last_prepared_certificate,
                          slow_nodes,
                          (0, CHK_FREQ + 1)))
    # check that fast_nodes ordered new txn
    looper.run(eventually(last_ordered_check,
                          fast_nodes,
                          (0, CHK_FREQ + 1)))
    # check that fast_nodes finalized first checkpoint and slow_nodes are not
    looper.run(eventually(check_checkpoint_finalize,
                          fast_nodes,
                          1, CHK_FREQ))
    for n in slow_nodes:
        assert not n.master_replica.checkpoints[(1, CHK_FREQ)].isStable

    # View change start emulation for change viewNo and fix last prepare
    # certificate, because if we start a real view change then checkpoints will
    # clean and the first checkpoint would not be need in finalizing.
    for node in txnPoolNodeSet:
        node.viewNo += 1
        node.master_replica.on_view_change_start()

    # reset delay for checkpoints
    reset_delay(slow_nodes, CHECKPOINT)
    # reset view change emulation and start real view change for finish it in
    # a normal mode with catchup
    for node in txnPoolNodeSet:
        node.viewNo -= 1
    ensure_view_change(looper, txnPoolNodeSet)
    for n in slow_nodes:
        assert not n.master_replica.checkpoints[(1, CHK_FREQ)].isStable
    # Check ordering the last txn before catchup. Check client reply is enough
    # because slow_nodes contains 3 nodes and without their replies sdk method
    # for get reply will not successfully finish.
    reset_delay(slow_nodes, COMMIT)
    sdk_get_and_check_replies(looper, requests)
    looper.run(eventually(last_ordered_check,
                          txnPoolNodeSet,
                          (0, CHK_FREQ + 1)))
    # check view change finish and checkpoints were cleaned
    ensureElectionsDone(looper, txnPoolNodeSet)
    for n in slow_nodes:
        assert (1, CHK_FREQ) not in n.master_replica.checkpoints
    # check that all nodes have same data after new txns ordering
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, CHK_FREQ)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)


def delay_msg(nodes, delay_function, types=None):
    for n in nodes:
        if types is None:
            n.nodeIbStasher.delay(delay_function(sys.maxsize))
        else:
            n.nodeIbStasher.delay(delay_function(sys.maxsize, types))


def reset_delay(nodes, message):
    for n in nodes:
        n.nodeIbStasher.reset_delays_and_process_delayeds(message)


def last_prepared_certificate(nodes, num):
    for n in nodes:
        assert n.master_replica.last_prepared_certificate_in_view() == num


def check_checkpoint_finalize(nodes, start_pp_seq_no, end_pp_seq_no):
    for n in nodes:
        checkpoint = n.master_replica.checkpoints[(start_pp_seq_no, end_pp_seq_no)]
        assert checkpoint.isStable


def last_ordered_check(nodes, last_ordered, instance_id=None):
    for n in nodes:
        last_ordered_3pc = n.master_last_ordered_3PC \
            if instance_id is None \
            else n.replicas[instance_id].last_ordered_3pc
        assert last_ordered_3pc == last_ordered

