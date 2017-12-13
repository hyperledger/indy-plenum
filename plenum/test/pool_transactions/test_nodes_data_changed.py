from plenum.common.constants import CLIENT_STACK_SUFFIX, DATA, ALIAS, \
    NODE_IP, NODE_PORT, CLIENT_PORT, CLIENT_IP, SERVICES, VALIDATOR
from plenum.common.util import randomString
from plenum.test.helper import waitRejectWithReason
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import addNewStewardAndNode, \
    sendUpdateNode, updateNodeDataAndReconnect
from plenum.test.test_node import checkNodesConnected

from stp_core.common.log import getlogger
from stp_core.network.port_dispenser import genHa

logger = getlogger()

# logged errors to ignore
whitelist = ['found legacy entry', "doesn't match", 'reconciling nodeReg',
             'missing', 'conflicts', 'matches', 'nodeReg',
             'conflicting address', 'unable to send message',
             'got error while verifying message']


# Whitelisting "got error while verifying message" since a node while not have
# initialised a connection for a new node by the time the new node's message
# reaches it


def testNodePortCannotBeChangedByAnotherSteward(looper, txnPoolNodeSet,
                                                steward1, stewardWallet,
                                                nodeThetaAdded):
    _, _, newNode = nodeThetaAdded
    nodeNewHa = genHa(1)
    new_port = nodeNewHa.port
    node_ha = txnPoolNodeSet[0].nodeReg[newNode.name]
    cli_ha = txnPoolNodeSet[0].cliNodeReg[newNode.name + CLIENT_STACK_SUFFIX]
    node_data = {
        ALIAS: newNode.name,
        NODE_PORT: new_port,
        NODE_IP: node_ha.host,
        CLIENT_PORT: cli_ha.port,
        CLIENT_IP: cli_ha.host,
    }

    logger.debug('{} changing port to {} {}'.format(newNode, new_port,
                                                    newNode.nodestack.ha.port))
    sendUpdateNode(steward1, stewardWallet, newNode,
                   node_data)

    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, steward1, 'is not a steward of node',
                             node.clientstack.name)


def test_node_alias_cannot_be_changed(looper, txnPoolNodeSet,
                                      nodeThetaAdded):
    """
    The node alias cannot be changed.
    """
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    node_data = {ALIAS: 'foo'}
    sendUpdateNode(newSteward, newStewardWallet, newNode,
                   node_data)
    for node in txnPoolNodeSet:
        waitRejectWithReason(looper, newSteward,
                             'data has conflicts with request data',
                             node.clientstack.name)


def testNodePortChanged(looper, txnPoolNodeSet, tdir, tconf,
                        steward1, stewardWallet, nodeThetaAdded):
    """
    An running node's port is changed
    """
    newSteward, newStewardWallet, newNode = nodeThetaAdded
    nodeNewHa = genHa(1)
    new_port = nodeNewHa.port

    node_ha = txnPoolNodeSet[0].nodeReg[newNode.name]
    cli_ha = txnPoolNodeSet[0].cliNodeReg[newNode.name + CLIENT_STACK_SUFFIX]
    node_data = {
        ALIAS: newNode.name,
        NODE_PORT: new_port,
        NODE_IP: node_ha.host,
        CLIENT_PORT: cli_ha.port,
        CLIENT_IP: cli_ha.host,
    }

    node = updateNodeDataAndReconnect(looper, newSteward,
                                      newStewardWallet, newNode,
                                      node_data,
                                      tdir, tconf,
                                      txnPoolNodeSet)

    waitNodeDataEquality(looper, node, *txnPoolNodeSet[:-1])

    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)


def testAddInactiveNodeThenActivate(looper, txnPoolNodeSet, tdir, client_tdir,
                                    tconf, steward1, stewardWallet, allPluginsPath):
    newStewardName = "testClientSteward" + randomString(3)
    newNodeName = "Kappa"

    # adding a new node without SERVICES field
    # it means the node is in the inactive state
    def del_services(op): del op[DATA][SERVICES]

    newSteward, newStewardWallet, newNode = \
        addNewStewardAndNode(looper,
                             steward1, stewardWallet,
                             newStewardName, newNodeName,
                             tdir, client_tdir, tconf, allPluginsPath,
                             transformNodeOpFunc=del_services)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # turn the new node on
    node_data = {
        ALIAS: newNode.name,
        SERVICES: [VALIDATOR]
    }

    updateNodeDataAndReconnect(looper, newSteward,
                               newStewardWallet, newNode,
                               node_data,
                               tdir, tconf,
                               txnPoolNodeSet + [newNode])
