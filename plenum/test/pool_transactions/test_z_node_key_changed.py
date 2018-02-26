import pytest
import base58
import types

from plenum.common import stack_manager
from plenum.common.keygen_utils import initNodeKeysForBothStacks, \
    initRemoteKeys
from plenum.common.signer_simple import SimpleSigner
from plenum.common.util import randomString
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.pool_transactions.helper import changeNodeKeys
from plenum.test.test_node import TestNode, checkNodesConnected
from plenum.common.config_helper import PNodeConfigHelper
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


def testNodeKeysChanged(looper, txnPoolNodeSet, tdir,
                        tconf, steward1, nodeThetaAdded,
                        allPluginsPath=None):
    newSteward, newStewardWallet, newNode = nodeThetaAdded

    newNode.stop()
    looper.removeProdable(name=newNode.name)
    nodeHa, nodeCHa = HA(*newNode.nodestack.ha), HA(*newNode.clientstack.ha)
    sigseed = randomString(32).encode()
    verkey = base58.b58encode(SimpleSigner(seed=sigseed).naclSigner.verraw)
    changeNodeKeys(looper, newSteward, newStewardWallet, newNode, verkey)

    config_helper = PNodeConfigHelper(newNode.name, tconf, chroot=tdir)
    initNodeKeysForBothStacks(newNode.name, config_helper.keys_dir, sigseed,
                              override=True)

    logger.debug("{} starting with HAs {} {}".format(newNode, nodeHa, nodeCHa))

    node = TestNode(newNode.name,
                    config_helper=config_helper,
                    config=tconf,
                    ha=nodeHa, cliha=nodeCHa, pluginPaths=allPluginsPath)
    looper.add(node)
    # The last element of `txnPoolNodeSet` is the node Theta that was just
    # stopped
    txnPoolNodeSet[-1] = node

    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, node, *txnPoolNodeSet[:-1])
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, steward1,
                                                  *txnPoolNodeSet)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newSteward,
                                                  *txnPoolNodeSet)


def testNodeInitRemoteKeysErrorsNotSuppressed(looper, txnPoolNodeSet,
                                              nodeThetaAdded, monkeypatch):

    TEST_EXCEPTION_MESSAGE = 'Failed to create some cert files'

    newSteward, newStewardWallet, newNode = nodeThetaAdded

    newNode.stop()
    looper.removeProdable(name=newNode.name)
    nodeHa, nodeCHa = HA(*newNode.nodestack.ha), HA(*newNode.clientstack.ha)
    sigseed = randomString(32).encode()
    verkey = base58.b58encode(SimpleSigner(seed=sigseed).naclSigner.verraw)

    def initRemoteKeysMock(name, *args, **kwargs):
        if name in [node.name for node in txnPoolNodeSet]:
            raise OSError(TEST_EXCEPTION_MESSAGE)
        else:
            return initRemoteKeys(name, *args, **kwargs)

    def wrap(node):
        oldMethod = node.poolManager.stackKeysChanged

        def stackKeysChanged(self, *args, **kwargs):
            with pytest.raises(OSError,
                               message="exception was suppressed") as excinfo:
                oldMethod(*args, **kwargs)
            excinfo.match(r'{}'.format(TEST_EXCEPTION_MESSAGE))
            return 0

        node.poolManager.stackKeysChanged = types.MethodType(stackKeysChanged,
                                                             node.poolManager)

    for node in txnPoolNodeSet:
        wrap(node)

    monkeypatch.setattr(stack_manager, 'initRemoteKeys', initRemoteKeysMock)

    changeNodeKeys(looper, newSteward, newStewardWallet, newNode, verkey)

    monkeypatch.undo()
