import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import CatchupRep
from plenum.test.delayers import delay_3pc_messages, pDelay, cDelay, ppDelay, \
    cr_delay
from plenum.test.helper import send_reqs_batches_and_get_suff_replies, \
    check_last_ordered_3pc, sdk_json_couples_to_request_list
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import getNonPrimaryReplicas, ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change


@pytest.fixture(scope='module', params=['some', 'all'])
def setup(request, looper, txnPoolNodeSet):
    slow_node = getNonPrimaryReplicas(txnPoolNodeSet, 0)[1].node
    fast_nodes = [n for n in txnPoolNodeSet if n != slow_node]
    # Delay catchup reply so that the test gets time to make the check,
    # this delay is reset after the check
    slow_node.nodeIbStasher.delay(cr_delay(100))
    slow_node.nodeIbStasher.delay(pDelay(100, 0))
    slow_node.nodeIbStasher.delay(cDelay(100, 0))
    if request.param == 'all':
        slow_node.nodeIbStasher.delay(ppDelay(100, 0))
    return slow_node, fast_nodes


def test_nodes_removes_request_keys_for_ordered(setup, looper, txnPoolNodeSet,
                                                sdk_pool_handle,
                                                sdk_wallet_client):
    """
    A node does not order requests since it is missing some 3PC messages,
    gets them from catchup. It then clears them from its request queues
    """
    slow_node, fast_nodes = setup

    reqs = sdk_json_couples_to_request_list(
        send_reqs_batches_and_get_suff_replies(
            looper, txnPoolNodeSet,
            sdk_pool_handle,
            sdk_wallet_client,
            10,
            5))
    ensure_all_nodes_have_same_data(looper, fast_nodes)
    assert slow_node.master_replica.last_ordered_3pc != \
           fast_nodes[0].master_replica.last_ordered_3pc

    def chk(key, nodes, present):
        for node in nodes:
            assert (
                           key in node.master_replica.requestQueues[DOMAIN_LEDGER_ID]) == present

    for req in reqs:
        chk(req.digest, fast_nodes, False)
        chk(req.digest, [slow_node], True)

    # Reset catchup reply delay so that  catchup can complete
    slow_node.nodeIbStasher.reset_delays_and_process_delayeds(CatchupRep.typename)

    old_last_ordered = fast_nodes[0].master_replica.last_ordered_3pc

    ensure_view_change(looper, txnPoolNodeSet)
    ensureElectionsDone(looper, txnPoolNodeSet)

    ensure_all_nodes_have_same_data(looper, fast_nodes)
    assert slow_node.master_replica.last_ordered_3pc == old_last_ordered

    for req in reqs:
        chk(req.digest, txnPoolNodeSet, False)

    # Needed for the next run due to the parametrised fixture
    slow_node.reset_delays_and_process_delayeds()
