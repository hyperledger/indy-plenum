import pytest
from plenum.common.constants import STEWARD_STRING, VALIDATOR
from pytest import fixture

from plenum.common.throughput_measurements import RevivalSpikeResistantEMAThroughputMeasurement
from plenum.common.util import getMaxFailures
from plenum.test.helper import sdk_send_random_and_check, assertExp, sdk_get_and_check_replies, waitForViewChange, \
    view_change_timeout
from plenum.test.node_catchup.helper import waitNodeDataEquality

from plenum.test.pool_transactions.conftest import sdk_node_theta_added
from plenum.test.pool_transactions.helper import sdk_add_new_nym, prepare_new_node_data, prepare_node_request, \
    sdk_sign_and_send_prepared_request, create_and_start_new_node, demote_node
from plenum.test.test_node import checkNodesConnected, TestNode, ensureElectionsDone
from stp_core.loop.eventually import eventually

nodeCount = 7


@pytest.fixture(scope="module")
def tconf(tconf):
    with view_change_timeout(tconf, 5):
        yield tconf


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

    # we are expecting 2 view changes here since Beta is selected as a master Primary on view=1
    # (since node reg at the beginning of view 0 is used to select it), but it's not available (demoted),
    # so we do view change to view=2 by timeout
    waitForViewChange(looper, txnPoolNodeSet, view_no + 2)
    ensureElectionsDone(looper, txnPoolNodeSet, customTimeout=30)

    waitNodeDataEquality(looper, *txnPoolNodeSet)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_stewards[0], 1)
    waitNodeDataEquality(looper, *txnPoolNodeSet)
