import pytest

from plenum.common.constants import LEDGER_STATUS
from plenum.test.delayers import cDelay, vcd_delay, lsDelay, msg_rep_delay
from plenum.test.helper import sdk_send_random_requests, check_last_ordered_3pc_on_master, max_3pc_batch_limits, \
    view_change_timeout
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import check_prepare_certificate, ensure_view_change
from plenum.test.view_change_with_delays.helper import check_last_prepared_certificate_after_view_change_start
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        with view_change_timeout(tconf, vc_timeout=420, catchup_timeout=300) as tconf:
            yield tconf


def test_view_change_by_order_stashed_on_all(txnPoolNodeSet, looper,
                                             sdk_pool_handle, sdk_wallet_steward):
    '''
    - COMMITS are delayed on all nodes
    - All nodes starts a view change with a prepared certificate (for delayed message)
    - COMMITS come during view change for all nodes,
    - So all nodes finish view change by processing Commits and Ordered msgs during view change (in between rounds of catchup).
    '''
    all_stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    initial_last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    txns_count = 4
    eventual_last_ordered = initial_last_ordered[0], initial_last_ordered[1] + txns_count

    with delay_rules(all_stashers, vcd_delay()):
        with delay_rules(all_stashers, cDelay()):
            with delay_rules(all_stashers, lsDelay(), msg_rep_delay(types_to_delay=[LEDGER_STATUS])):
                sdk_send_random_requests(looper, sdk_pool_handle,
                                         sdk_wallet_steward, txns_count)

                looper.run(eventually(check_prepare_certificate, txnPoolNodeSet, txns_count))
                check_last_ordered_3pc_on_master(txnPoolNodeSet, initial_last_ordered)

                # trigger view change on all nodes
                ensure_view_change(looper, txnPoolNodeSet)

                looper.run(eventually(check_last_prepared_certificate_after_view_change_start, txnPoolNodeSet,
                                      eventual_last_ordered))

        # check that all txns are ordered till last prepared
        looper.run(eventually(check_last_ordered_3pc_on_master, txnPoolNodeSet, eventual_last_ordered, timeout=30))

    # wait for view change done on all nodes
    ensureElectionsDone(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    # make sure that the pool is functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)


def test_view_change_by_order_stashed_on_3_nodes_and_catchup_on_1_node(txnPoolNodeSet, looper,
                                                                       sdk_pool_handle, sdk_wallet_steward):
    '''
    - COMMITS are delayed on all nodes
    - All nodes starts a view change with a prepared certificate (for delayed message)
    - COMMITS come during view change for 3 nodes
    - So these 3 nodes finish view change by processing Commits and Ordered msgs during view change (in between rounds of catchup).
    - The lagging (4th) node receives missing txns as part of catch-up (during view change) and also finishes view change.
    '''
    slow_node = txnPoolNodeSet[-1]
    fast_nodes = txnPoolNodeSet[:-1]
    slow_stasher = slow_node.nodeIbStasher
    fast_stashers = [n.nodeIbStasher for n in fast_nodes]
    all_stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    initial_last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    txns_count = 4
    eventual_last_ordered = initial_last_ordered[0], initial_last_ordered[1] + txns_count

    with delay_rules(all_stashers, vcd_delay()):
        # the lagging node is slow in receiving Commits and Catchup mghs
        with delay_rules(slow_stasher, cDelay()):
            with delay_rules(slow_stasher, lsDelay(), msg_rep_delay(types_to_delay=[LEDGER_STATUS])):
                # fast nodes will receive and order Commits for last_prepared_cert during view change
                with delay_rules(fast_stashers, cDelay()):
                    with delay_rules(fast_stashers, lsDelay(), msg_rep_delay(types_to_delay=[LEDGER_STATUS])):
                        sdk_send_random_requests(looper, sdk_pool_handle,
                                                 sdk_wallet_steward, txns_count)

                        looper.run(eventually(check_prepare_certificate, txnPoolNodeSet, txns_count))
                        check_last_ordered_3pc_on_master(txnPoolNodeSet, initial_last_ordered)

                        # trigger view change on all nodes
                        ensure_view_change(looper, txnPoolNodeSet)

                        looper.run(eventually(check_last_prepared_certificate_after_view_change_start,
                                              txnPoolNodeSet, eventual_last_ordered))

                # check that all txns are ordered till last prepared on fast nodes
                looper.run(eventually(check_last_ordered_3pc_on_master, fast_nodes, eventual_last_ordered, timeout=30))

            # check that all txns are ordered till last prepared on slow node as a result of catchup
            looper.run(eventually(check_last_ordered_3pc_on_master, [slow_node], eventual_last_ordered, timeout=30))

    # wait for view change done on all nodes
    ensureElectionsDone(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    # make sure that the pool is functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
