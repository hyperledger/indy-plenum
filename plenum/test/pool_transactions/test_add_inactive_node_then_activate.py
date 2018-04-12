import pytest

from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.common.util import randomString
from plenum.test.pool_transactions.helper import sdk_add_new_steward_and_node, sdk_pool_refresh, \
    update_node_data_and_reconnect
from plenum.test.test_node import checkNodesConnected
from stp_core.common.log import getlogger


logger = getlogger()


def testAddInactiveNodeThenActivate(looper, txnPoolNodeSet,
                                    sdk_wallet_steward,
                                    sdk_pool_handle, tdir, tconf, allPluginsPath):
    new_steward_name = "testClientSteward" + randomString(3)
    new_node_name = "Kappa"

    # adding a new node without SERVICES field
    # it means the node is in the inactive state
    new_steward_wallet, new_node = \
        sdk_add_new_steward_and_node(looper,
                                     sdk_pool_handle,
                                     sdk_wallet_steward,
                                     new_steward_name,
                                     new_node_name,
                                     tdir,
                                     tconf,
                                     allPluginsPath,
                                     services=None)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    sdk_pool_refresh(looper, sdk_pool_handle)
    new_node = update_node_data_and_reconnect(looper, txnPoolNodeSet + [new_node],
                                              new_steward_wallet,
                                              sdk_pool_handle,
                                              new_node,
                                              None, None,
                                              None, None,
                                              tdir, tconf)
    txnPoolNodeSet.append(new_node)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, new_steward_wallet, sdk_pool_handle)
