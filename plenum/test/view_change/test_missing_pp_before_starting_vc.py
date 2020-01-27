import pytest

from plenum.common.messages.node_messages import PrePrepare
from plenum.test.delayers import delay_3pc
from plenum.test.helper import sdk_send_random_requests, check_missing_pre_prepares, max_3pc_batch_limits
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone, check_not_in_view_change
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf


def test_missing_pp_before_starting_vc(tconf, txnPoolNodeSet, looper,
                                       sdk_pool_handle, sdk_wallet_steward):
    '''
    - all nodes delay PrePrepares for viewNo=1 with ppSeqNo<4
    - all nodes go to view=1
    - 10 batches are sent (they will not be ordered because of missing (delayed) PrePrepares
    - all nodes start a view change to view=2
    - So all nodes finish view change successfully and can continue ordering
    '''
    all_stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    # 1. delay PrePrepares with ppSeqNo<4
    with delay_rules(all_stashers,
                     delay_3pc(view_no=1, before=4, msgs=PrePrepare)):
        # 2. do view change for view=1
        ensure_view_change(looper, txnPoolNodeSet)
        looper.run(eventually(check_not_in_view_change, txnPoolNodeSet))

        # 3. send requests
        sdk_send_random_requests(looper, sdk_pool_handle,
                                 sdk_wallet_steward, 10)

        # 4. do view change for view=2
        ensure_view_change(looper, txnPoolNodeSet)

    # 5. ensure everything is fine
    ensureElectionsDone(looper, txnPoolNodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, sdk_wallet_steward, sdk_pool_handle)
