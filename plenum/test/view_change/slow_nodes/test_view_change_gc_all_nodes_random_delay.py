from itertools import combinations

from stp_core.loop.eventually import eventually

from plenum.test import waits
from plenum.test.helper import sendRandomRequests, \
    send_reqs_to_nodes_and_verify_all_replies, \
    waitForSufficientRepliesForRequests, \
    checkViewNoForNodes, \
    check_last_ordered_3pc
from plenum.test.delayers import delay_3pc_messages, \
    reset_delays_and_process_delayeds
from plenum.test.view_change.helper import ensure_view_change_complete
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper


def check_nodes_last_ordered_3pc(nodes, last_ordered_3pc):
    for n1, n2 in combinations(nodes, 2):
        lst_3pc = check_last_ordered_3pc(n1, n2)

    assert lst_3pc == last_ordered_3pc


def check_nodes_requests_size(nodes, size):
    for node in nodes:
        assert len(node.requests) == size


def test_view_change_gc_in_between_3pc_all_nodes_delays(
        looper, txnPoolNodeSet, wallet1, client):
    """
    Test that garbage collector compares the whole 3PC key (viewNo, ppSeqNo)
    and does not remove messages from node's queues that have higher
    viewNo than last ordered one even if their ppSeqNo are less or equal
    """

    numNodes = len(client.nodeReg)
    viewNo = checkViewNoForNodes(txnPoolNodeSet)

    # 1 send two messages one by one separately to make
    #  node pool working with two batches
    #    -> last_ordered_3pc = (+0, 2) [+0 means from the initial state]
    #       (last_ordered_3pc here and futher is tracked
    #       for master instances only cause non-master ones have
    #       specific logic of its management which we don't care in
    #       the test, see Replica::_setup_for_non_master)
    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client, 1)
    send_reqs_to_nodes_and_verify_all_replies(looper, wallet1, client, 1)

    last_ordered_3pc = (viewNo, 2)
    check_nodes_last_ordered_3pc(txnPoolNodeSet, last_ordered_3pc)
    check_nodes_requests_size(txnPoolNodeSet, 2)

    # 2 do view change
    #    -> GC should remove it from nodes' queues
    #    -> viewNo = +1
    ensure_view_change_complete(looper, txnPoolNodeSet)

    viewNo = checkViewNoForNodes(txnPoolNodeSet, viewNo + 1)
    check_nodes_last_ordered_3pc(txnPoolNodeSet, last_ordered_3pc)
    check_nodes_requests_size(txnPoolNodeSet, 0)

    # 3 slow processing 3PC messages for all nodes (all replica instances)
    #   randomly and send one more message
    #    -> not ordered (last_ordered_3pc still equal (+0, 2)) but primaries
    #       should at least send PRE-PREPAREs
    # TODO could it be not enough for wainting that at least primary
    # has sent PRE-PREPARE
    propagationTimeout = waits.expectedClientRequestPropagationTime(numNodes)
    delay_3pc_messages(txnPoolNodeSet,
                       0,
                       delay=propagationTimeout * 2)
    delay_3pc_messages(txnPoolNodeSet,
                       1,
                       delay=propagationTimeout * 2)
    requests = sendRandomRequests(wallet1, client, 1)

    def checkPrePrepareSentAtLeastByPrimary():
        for node in txnPoolNodeSet:
            for replica in node.replicas:
                if replica.isPrimary:
                    assert len(replica.sentPrePrepares)

    looper.run(eventually(checkPrePrepareSentAtLeastByPrimary,
                          retryWait=0.1,
                          timeout=propagationTimeout))
    # 4 do view change
    #    -> GC shouldn't remove anything because
    #       last_ordered_3pc (+0, 1) < last message's 3pc key (+1, 1)
    #    -> viewNo = 2
    ensure_view_change_complete(looper, txnPoolNodeSet)

    viewNoNew = checkViewNoForNodes(txnPoolNodeSet)
    # another view change could happen because of slow nodes
    assert viewNoNew - viewNo in (1, 2)
    viewNo = viewNoNew
    check_nodes_last_ordered_3pc(txnPoolNodeSet, last_ordered_3pc)
    check_nodes_requests_size(txnPoolNodeSet, 1)

    # 5 reset delays and wait for replies
    #    -> new primaries should send new 3pc for last message
    #       with 3pc key (+2, 1)
    #    -> they should be ordered
    #    -> last_ordered_3pc = (+2, 1)
    reset_delays_and_process_delayeds(txnPoolNodeSet)
    waitForSufficientRepliesForRequests(looper, client,
                                        requests=requests)

    checkViewNoForNodes(txnPoolNodeSet, viewNo)
    last_ordered_3pc = (viewNo, 1)
    check_nodes_last_ordered_3pc(txnPoolNodeSet, last_ordered_3pc)
    check_nodes_requests_size(txnPoolNodeSet, 1)

    # 6 do view change
    #    -> GC should remove them
    ensure_view_change_complete(looper, txnPoolNodeSet)

    viewNo = checkViewNoForNodes(txnPoolNodeSet, viewNo + 1)
    check_nodes_last_ordered_3pc(txnPoolNodeSet, last_ordered_3pc)
    check_nodes_requests_size(txnPoolNodeSet, 0)
