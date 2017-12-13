import pytest
from plenum.test.delayers import ppgDelay
from plenum.test.helper import send_pre_prepare, send_prepare, send_commit, \
    sendRandomRequests, waitRejectFromPoolWithReason, \
    waitForSufficientRepliesForRequests
from plenum.test.test_node import getPrimaryReplica
from plenum.test.view_change.helper import check_replica_queue_empty, \
    check_all_replica_queue_empty


@pytest.mark.skip('Currently we stash client requests during view change')
def test_no_requests_processed_during_view_change(looper, nodeSet,
                                                  client1, wallet1):
    for node in nodeSet:
        node.view_change_in_progress = True

    sendRandomRequests(wallet1, client1, 10)

    waitRejectFromPoolWithReason(
        looper,
        nodeSet,
        client1,
        'Can not process requests when view change is in progress')

    for node in nodeSet:
        check_replica_queue_empty(node)


@pytest.mark.skip('The filter is not enabled now')
def test_no_new_view_3pc_messages_processed_during_view_change(
        looper, nodeSet, client1, wallet1):
    for node in nodeSet:
        node.view_change_in_progress = True

    new_view_no = getPrimaryReplica(nodeSet).node.viewNo + 1
    pp_seq_no = 1

    send_pre_prepare(new_view_no, pp_seq_no, wallet1, nodeSet)
    looper.runFor(1)
    check_all_replica_queue_empty(nodeSet)

    send_prepare(new_view_no, pp_seq_no, nodeSet)
    looper.runFor(1)
    check_all_replica_queue_empty(nodeSet)

    send_commit(new_view_no, pp_seq_no, nodeSet)
    looper.runFor(1)
    check_all_replica_queue_empty(nodeSet)


@pytest.mark.skip('The filter is not enabled now')
def test_old_view_requests_processed_during_view_change(looper, nodeSet,
                                                        client1, wallet1):
    """
    Make sure that requests sent before view change started are processed and replies are returned:
     - delay Propogates (to make sure that requests are not ordered before view change is started)
     - send requests
     - check that requests are ordered despite of view change being in progress
    """
    for node in nodeSet:
        node.view_change_in_progress = False
        node.nodeIbStasher.delay(ppgDelay(3, 0))

    reqs = sendRandomRequests(wallet1, client1, 2)
    looper.runFor(1)

    for node in nodeSet:
        node.view_change_in_progress = True

    waitForSufficientRepliesForRequests(looper, client1, requests=reqs)
