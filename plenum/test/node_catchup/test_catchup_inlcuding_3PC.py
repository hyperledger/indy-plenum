import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import check_if_all_equal_in_list
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.node_catchup.helper import check_last_3pc_master, \
    waitNodeDataEquality
from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.eventually import eventually

TestRunningTimeLimitSec = 125


def test_nodes_maintain_master_txn_3PC_map(looper, txnPoolNodeSet,
                                           sdk_node_created_after_some_txns):
    _, new_node, sdk_pool_handle, new_steward_wallet_handle = \
        sdk_node_created_after_some_txns
    txnPoolNodeSet.append(new_node)

    # Check the new node has set same `last_3pc_ordered` for master as others
    check_last_3pc_master(new_node, txnPoolNodeSet[:4])
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])

    # check that the node has the same primaries
    ensureElectionsDone(looper=looper,
                        nodes=txnPoolNodeSet)

    # Requests still processed
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              new_steward_wallet_handle, 2)
    waitNodeDataEquality(looper, new_node, *txnPoolNodeSet[:4])
