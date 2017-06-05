import base58
from plenum.common.keygen_utils import initNodeKeysForBothStacks
from plenum.common.signer_simple import SimpleSigner
from plenum.common.util import randomString
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import changeNodeKeys
from plenum.test.test_node import TestNode, checkNodesConnected

from stp_core.common.log import getlogger
from stp_core.types import HA

logger = getlogger()

# logged errors to ignore
whitelist = ['found legacy entry', "doesn't match", 'reconciling nodeReg',
             'missing', 'conflicts', 'matches', 'nodeReg',
             'conflicting address', 'unable to send message',
             'got error while verifying message']
# Whitelisting "got error while verifying message" since a node while not have
# initialised a connection for a new node by the time the new node's message
# reaches it


def testNodeKeysChanged(looper, txnPoolNodeSet, tdirWithPoolTxns,
                        tconf, steward1, nodeThetaAdded,
                        allPluginsPath=None):
    newSteward, newStewardWallet, newNode = nodeThetaAdded

    newNode.stop()
    looper.removeProdable(name=newNode.name)
    nodeHa, nodeCHa = HA(*newNode.nodestack.ha), HA(*newNode.clientstack.ha)
    sigseed = randomString(32).encode()
    verkey = base58.b58encode(SimpleSigner(seed=sigseed).naclSigner.verraw)
    changeNodeKeys(looper, newSteward, newStewardWallet, newNode, verkey)
    initNodeKeysForBothStacks(newNode.name, tdirWithPoolTxns, sigseed,
                              override=True)

    logger.debug("{} starting with HAs {} {}".format(newNode, nodeHa, nodeCHa))
    node = TestNode(newNode.name, basedirpath=tdirWithPoolTxns, config=tconf,
                    ha=nodeHa, cliha=nodeCHa, pluginPaths=allPluginsPath)
    looper.add(node)
    # The last element of `txnPoolNodeSet` is the node Theta that was just
    # stopped
    txnPoolNodeSet[-1] = node

    looper.run(checkNodesConnected(stacks=txnPoolNodeSet))
    waitNodeDataEquality(looper, node, *txnPoolNodeSet[:-1])
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)

