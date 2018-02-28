import pytest
from plenum.test.node_request.helper import sdk_ensure_pool_functional

from plenum.common.constants import CLIENT_STACK_SUFFIX, DATA, ALIAS, \
    NODE_IP, NODE_PORT, CLIENT_PORT, CLIENT_IP, SERVICES, VALIDATOR
from plenum.common.util import randomString, hexToFriendly
from plenum.test.helper import waitRejectWithReason, sdk_send_random_and_check
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import addNewStewardAndNode, \
    sendUpdateNode, sdk_send_update_node_HAs, sdk_add_new_steward_and_node, \
    sdk_pool_refresh, update_node_data_and_reconnect
from plenum.test.test_node import checkNodesConnected

from stp_core.common.log import getlogger
from stp_core.network.port_dispenser import genHa

logger = getlogger()


# Whitelisting "got error while verifying message" since a node while not have
# initialised a connection for a new node by the time the new node's message
# reaches it


def testNodePortCannotBeChangedByAnotherSteward(looper, txnPoolNodeSet,
                                                sdk_wallet_steward,
                                                sdk_pool_handle,
                                                sdk_node_theta_added):
    new_steward_wallet, new_node = sdk_node_theta_added
    node_new_ha, client_new_ha = genHa(2)
    logger.debug("{} changing HAs to {} {}".format(new_node, node_new_ha,
                                                   client_new_ha))
    node_dest = hexToFriendly(new_node.nodestack.verhex)
    with pytest.raises(AssertionError):
        sdk_send_update_node_HAs(looper, sdk_wallet_steward, sdk_pool_handle,
                                 node_dest, new_node.name,
                                 node_new_ha.host, node_new_ha.port,
                                 client_new_ha.host, client_new_ha.port)
    sdk_pool_refresh(looper, sdk_pool_handle)


def test_node_alias_cannot_be_changed(looper, txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_node_theta_added):
    """
    The node alias cannot be changed.
    """
    new_steward_wallet, new_node = sdk_node_theta_added
    node_dest = hexToFriendly(new_node.nodestack.verhex)
    with pytest.raises(AssertionError):
        sdk_send_update_node_HAs(looper, new_steward_wallet, sdk_pool_handle,
                                 node_dest, 'foo',
                                 None, None,
                                 None, None)
    sdk_pool_refresh(looper, sdk_pool_handle)


def testNodePortChanged(looper, txnPoolNodeSet,
                        sdk_wallet_steward,
                        sdk_pool_handle,
                        sdk_node_theta_added,
                        tdir, tconf):
    """
    An running node's port is changed
    """
    new_steward_wallet, new_node = sdk_node_theta_added

    node_new_ha = genHa(1)
    new_port = node_new_ha.port
    node_ha = txnPoolNodeSet[0].nodeReg[new_node.name]
    cli_ha = txnPoolNodeSet[0].cliNodeReg[new_node.name + CLIENT_STACK_SUFFIX]

    update_node_data_and_reconnect(looper, txnPoolNodeSet,
                                              new_steward_wallet,
                                              sdk_pool_handle,
                                              new_node,
                                              node_ha.host, new_port,
                                              cli_ha.host, cli_ha.port,
                                              tdir, tconf)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, new_steward_wallet, sdk_pool_handle)


# !!! THIS TEST DON'T WORK WHEN STARTED WITH ALL TESTS IN THIS FILE
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
    txnPoolNodeSet.append(new_node)
    node_ha = new_node.nodeReg[new_node.name]
    cli_ha = new_node.cliNodeReg[new_node.name + CLIENT_STACK_SUFFIX]
    new_node = update_node_data_and_reconnect(looper, txnPoolNodeSet,
                                   new_steward_wallet,
                                   sdk_pool_handle,
                                   new_node,
                                   node_ha.host, node_ha.port,
                                   cli_ha.host, cli_ha.port,
                                   tdir, tconf)
    # txnPoolNodeSet.append(new_node)
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, new_steward_wallet, sdk_pool_handle)
