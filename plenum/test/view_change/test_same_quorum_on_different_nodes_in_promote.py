import pytest as pytest

from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.constants import STEWARD_STRING, INSTANCE_CHANGE

from plenum.test.delayers import cDelay, ppDelay, pDelay, nv_delay, icDelay
from plenum.test.helper import waitForViewChange, checkViewNoForNodes
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node, sdk_add_new_nym, prepare_new_node_data, \
    prepare_node_request, sdk_sign_and_send_prepared_request, create_and_start_new_node
from plenum.test.stasher import delay_rules_without_processing, delay_rules
from plenum.test.test_node import checkNodesConnected, TestNode, ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change_complete


nodeCount = 4


@pytest.mark.skip(reason="INDY-2322 A lagging node may be the only one who started view change "
                         "in case of F Nodes added/promoted in 1 batch")
def test_same_quorum_on_different_nodes_in_promote(looper, txnPoolNodeSet, sdk_pool_handle,
                                                   sdk_wallet_steward, tdir, tconf, allPluginsPath):
    view_no = txnPoolNodeSet[-1].viewNo

    # Add a New node but don't allow Delta to be aware it. We do not want it in Delta's node registry.
    with delay_rules_without_processing(txnPoolNodeSet[-1].nodeIbStasher, ppDelay(), pDelay(), cDelay()):
        _, new_node = sdk_add_new_steward_and_node(looper, sdk_pool_handle, sdk_wallet_steward,
                                                   'New_Steward', 'Epsilon',
                                                   tdir, tconf, allPluginsPath=allPluginsPath)
        txnPoolNodeSet.append(new_node)

        nodes_sans_alpha = txnPoolNodeSet[-1:]
        looper.run(checkNodesConnected(nodes_sans_alpha))

    waitForViewChange(looper, txnPoolNodeSet, view_no + 1)
    ensure_view_change_complete(looper, txnPoolNodeSet)
