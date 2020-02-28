import pytest
from plenum.test.test_node import getPrimaryReplica

from plenum.common.exceptions import RequestRejectedException
from plenum.test.delayers import ppgDelay
from plenum.test.helper import sdk_send_random_and_check, sdk_send_random_requests, sdk_get_replies, send_pre_prepare, \
    send_prepare, send_commit
from plenum.test.view_change.helper import check_replica_queue_empty, \
    check_all_replica_queue_empty


@pytest.mark.skip('Currently we stash client requests during view change')
def test_no_requests_processed_during_view_change(looper, txnPoolNodeSet,
                                                  sdk_pool_handle, sdk_wallet_client):
    for node in txnPoolNodeSet:
        node.view_change_in_progress = True

    with pytest.raises(RequestRejectedException) as e:
        sdk_send_random_and_check(looper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_client, 10)
    assert 'Can not process requests when view change is in progress' in \
           e._excinfo[1].args[0]

    for node in txnPoolNodeSet:
        check_replica_queue_empty(node)


@pytest.mark.skip('The filter is not enabled now')
def test_no_new_view_3pc_messages_processed_during_view_change(
        looper, txnPoolNodeSet):
    for node in txnPoolNodeSet:
        node.view_change_in_progress = True

    new_view_no = getPrimaryReplica(txnPoolNodeSet).node.viewNo + 1
    pp_seq_no = 1

    send_pre_prepare(new_view_no, pp_seq_no, txnPoolNodeSet)
    looper.runFor(1)
    check_all_replica_queue_empty(txnPoolNodeSet)

    send_prepare(new_view_no, pp_seq_no, txnPoolNodeSet)
    looper.runFor(1)
    check_all_replica_queue_empty(txnPoolNodeSet)

    send_commit(new_view_no, pp_seq_no, txnPoolNodeSet)
    looper.runFor(1)
    check_all_replica_queue_empty(txnPoolNodeSet)


@pytest.mark.skip('The filter is not enabled now')
def test_old_view_requests_processed_during_view_change(looper, txnPoolNodeSet,
                                                        sdk_wallet_handle,
                                                        sdk_wallet_client):
    """
    Make sure that requests sent before view change started are processed and replies are returned:
     - delay Propogates (to make sure that requests are not ordered before view change is started)
     - send requests
     - check that requests are ordered despite of view change being in progress
    """
    for node in txnPoolNodeSet:
        node.view_change_in_progress = False
        node.nodeIbStasher.delay(ppgDelay(3, 0))

    requests = sdk_send_random_requests(looper, sdk_wallet_handle,
                                        sdk_wallet_client, 2)
    looper.runFor(1)

    for node in txnPoolNodeSet:
        node.view_change_in_progress = True

    sdk_get_replies(looper, requests)
