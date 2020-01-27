import sys

from plenum.common.constants import PREPREPARE
from plenum.test.delayers import ppDelay, msg_rep_delay
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_send_random_request
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import check_prepare_certificate
from plenum.test.view_change_service.helper import trigger_view_change
from stp_core.loop.eventually import eventually


def test_view_change_with_different_prepare_certificate(looper, txnPoolNodeSet,
                                                        sdk_pool_handle,
                                                        sdk_wallet_client):
    """
    Check that a node without pre-prepare but with quorum of prepares wouldn't
    use this transaction as a last in prepare certificate
    """
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    slow_node = txnPoolNodeSet[-1]
    # delay preprepares and message response with preprepares.
    with delay_rules(slow_node.nodeIbStasher, ppDelay(delay=sys.maxsize)):
        with delay_rules(slow_node.nodeIbStasher,
                         msg_rep_delay(delay=sys.maxsize,
                                       types_to_delay=[PREPREPARE, ])):
            last_ordered = slow_node.master_replica.last_ordered_3pc
            sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)
            looper.run(eventually(check_prepare_certificate,
                                  txnPoolNodeSet[0:-1],
                                  last_ordered[1] + 1))

            trigger_view_change(txnPoolNodeSet)
            assert slow_node.master_replica._ordering_service.l_last_prepared_certificate_in_view() == \
                   (0, last_ordered[1])
            ensureElectionsDone(looper, txnPoolNodeSet)

    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_client, sdk_pool_handle)
