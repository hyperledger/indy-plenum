from plenum.test.helper import sendRandomRequests, waitRejectFromPoolWithReason, send_pre_prepare, send_prepare, \
    send_commit
from plenum.test.test_node import getPrimaryReplica
from plenum.test.view_change.helper import check_replica_queue_empty, check_all_replica_queue_empty


def test_no_requests_processed_during_view_change(looper, nodeSet,
                                                  client1, wallet1):
    for node in nodeSet:
        node.view_change_in_progress = True

    sendRandomRequests(wallet1, client1, 10)

    waitRejectFromPoolWithReason(looper, nodeSet, client1, 'view change in progress')

    for node in nodeSet:
        check_replica_queue_empty(node)


def test_no_new_view_3pc_messages_processed_during_view_change(looper, nodeSet,
                                                               client1, wallet1):
    #for node in nodeSet:
    #    node.view_change_in_progress = True

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


def test_old_view_3pc_messages_processed_during_view_change(looper, nodeSet,
                                                            client1, wallet1):
    #for node in nodeSet:
    #    node.view_change_in_progress = True

    view_no = getPrimaryReplica(nodeSet).node.viewNo
    pp_seq_no = 1

    send_pre_prepare(view_no, pp_seq_no, wallet1, nodeSet)
    looper.runFor(1)
    check_all_replica_queue_empty(nodeSet)

    send_prepare(view_no, pp_seq_no, nodeSet)
    looper.runFor(1)
    check_all_replica_queue_empty(nodeSet)

    send_commit(view_no, pp_seq_no, nodeSet)
    looper.runFor(1)
    check_all_replica_queue_empty(nodeSet)


def test_catch_up_processed_messages(looper, txnPoolNodeSet,
                                     client1, wallet1, client1Connected):
    pass
