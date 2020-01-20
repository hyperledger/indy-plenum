import pytest
from plenum.test.test_node import ensureElectionsDone

from plenum.common.messages.node_messages import NewView
from plenum.test.helper import sdk_send_random_and_check, waitForViewChange
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.view_change_service.helper import trigger_view_change

call_count = 0


@pytest.fixture(scope="module")
def tconf(tconf):
    old_timeout = tconf.NEW_VIEW_TIMEOUT
    tconf.NEW_VIEW_TIMEOUT = 7
    yield tconf

    tconf.NEW_VIEW_TIMEOUT = old_timeout


@pytest.fixture(scope='function', params=range(1, 5))
def lost_count(request):
    return request.param


def test_view_change_with_lost_new_view(txnPoolNodeSet,
                                        looper,
                                        sdk_pool_handle,
                                        sdk_wallet_steward,
                                        tconf,
                                        tdir,
                                        lost_count):
    '''
    Skip processing of lost_count Message Responses with NewView
    in view change; test makes sure that the node eventually finishes view change
    '''

    node_to_disconnect = txnPoolNodeSet[-1]
    initial_view_no = txnPoolNodeSet[0].viewNo

    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_steward, 5)

    def unpatch_after_call(msg, frm):
        global call_count
        call_count += 1

        if call_count >= lost_count:
            # unpatch sendToReplica for NewView after lost_count calls
            node_to_disconnect.nodeMsgRouter.add((NewView, node_to_disconnect.sendToReplica))
            call_count = 0

    # patch sendToReplica for routing NewView
    node_to_disconnect.nodeMsgRouter.add((NewView, unpatch_after_call))

    # trigger view change on all nodes
    trigger_view_change(txnPoolNodeSet)
    waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=initial_view_no + 1)
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet,
                        customTimeout=tconf.NEW_VIEW_TIMEOUT * (call_count + 1) + 5)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    # make sure that the pool is functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
