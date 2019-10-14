
from plenum.test.delayers import nv_delay
from plenum.test.helper import sdk_send_random_requests, sdk_send_batches_of_random_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules_without_processing
from plenum.test.test_node import ensureElectionsDone
from plenum.test.checkpoints.conftest import chkFreqPatched


CHK_FREQ = 5
LOG_SIZE = 3 * CHK_FREQ


def test_catchup_in_view_change_by_checkpoints(txnPoolNodeSet, looper,
                                             sdk_pool_handle, sdk_wallet_steward,
                                               chkFreqPatched):
    '''
    Delay (without processing) receiving of NewView by Gamma
    Start a view change and wait until it finished on all nodes except Gamma
    Order by all nodes except Gamma. Order 2 stable checkpoints
    Make sure that Gamma eventually caught up all transactions, finished view change, and can participate in ordering
    '''
    slow_node = txnPoolNodeSet[-1]
    fast_nodes = txnPoolNodeSet[:-1]

    sdk_send_random_requests(looper, sdk_pool_handle,
                             sdk_wallet_steward, 1)

    with delay_rules_without_processing(slow_node.nodeIbStasher, nv_delay()):
        # trigger view change on all nodes
        for n in txnPoolNodeSet:
            n.view_changer.on_master_degradation()
        ensureElectionsDone(looper, fast_nodes,
                            instances_list=fast_nodes[0].replicas.keys())
        ensure_all_nodes_have_same_data(looper, nodes=fast_nodes)

        sdk_send_random_requests(looper, sdk_pool_handle,
                                 sdk_wallet_steward, 1)
    assert slow_node.view_change_in_progress

    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                         sdk_wallet_steward,
                                         num_reqs=CHK_FREQ * 2 - 2,
                                         num_batches=CHK_FREQ * 2 - 2)

    # wait for view change done on all nodes
    ensureElectionsDone(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)

    # make sure that the pool is functional
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
