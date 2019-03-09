import pytest
from plenum.common.constants import STEWARD_STRING, VALIDATOR
from pytest import fixture

from plenum.common.throughput_measurements import RevivalSpikeResistantEMAThroughputMeasurement
from plenum.common.util import getMaxFailures
from plenum.test.helper import sdk_send_random_and_check, assertExp, sdk_get_and_check_replies
from plenum.test.node_catchup.helper import waitNodeDataEquality

from plenum.test.pool_transactions.conftest import sdk_node_theta_added
from plenum.test.pool_transactions.helper import sdk_add_new_nym, prepare_new_node_data, prepare_node_request, \
    sdk_sign_and_send_prepared_request, create_and_start_new_node, demote_node
from plenum.test.test_node import checkNodesConnected, TestNode
from stp_core.loop.eventually import eventually

nodeCount = 7

def test_catchup_after_replica_removing(looper, sdk_pool_handle, txnPoolNodeSet,
                                        sdk_wallet_stewards, tdir, tconf, allPluginsPath):
    view_no = txnPoolNodeSet[-1].viewNo
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle, sdk_wallet_stewards[0], 1)
    waitNodeDataEquality(looper, *txnPoolNodeSet)

    index, node_for_demote = [(i, n) for i, n in enumerate(txnPoolNodeSet) if n.replicas[1].isPrimary][0]
    sdk_wallet_steward = sdk_wallet_stewards[index]
    demote_node(looper, sdk_wallet_steward, sdk_pool_handle, node_for_demote)
    txnPoolNodeSet.pop(index)

    looper.run(eventually(lambda: assertExp(n.viewNo == view_no for n in txnPoolNodeSet)))
    waitNodeDataEquality(looper, *txnPoolNodeSet)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_stewards[0], 1)
    waitNodeDataEquality(looper, *txnPoolNodeSet)
