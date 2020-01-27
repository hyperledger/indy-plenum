import pytest
import base58
import types

from plenum.test.node_request.helper import sdk_ensure_pool_functional

from plenum.common import stack_manager
from plenum.common.keygen_utils import initNodeKeysForBothStacks, \
    initRemoteKeys
from plenum.common.signer_simple import SimpleSigner
from plenum.common.util import randomString
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import sdk_change_node_keys
from plenum.test.test_node import TestNode, checkNodesConnected
from plenum.common.config_helper import PNodeConfigHelper
from stp_core.common.log import getlogger
from stp_core.types import HA

logger = getlogger()


# Whitelisting "got error while verifying message" since a node while not have
# initialised a connection for a new node by the time the new node's message
# reaches it


def testNodeKeysChanged(looper, txnPoolNodeSet, tdir,
                        tconf, sdk_node_theta_added,
                        sdk_pool_handle,
                        allPluginsPath=None):
    # 1. Add new node
    orig_view_no = txnPoolNodeSet[0].viewNo
    new_steward_wallet, new_node = sdk_node_theta_added

    # 2. Stop new node and rotate its keys
    new_node.stop()
    looper.removeProdable(name=new_node.name)
    nodeHa, nodeCHa = HA(*new_node.nodestack.ha), HA(*new_node.clientstack.ha)
    sigseed = randomString(32).encode()
    verkey = base58.b58encode(SimpleSigner(seed=sigseed).naclSigner.verraw).decode("utf-8")
    sdk_change_node_keys(looper, new_node, new_steward_wallet, sdk_pool_handle, verkey)

    # 3. Start the new node back with the new keys
    logger.debug("{} starting with HAs {} {}".format(new_node, nodeHa, nodeCHa))
    config_helper = PNodeConfigHelper(new_node.name, tconf, chroot=tdir)
    initNodeKeysForBothStacks(new_node.name, config_helper.keys_dir, sigseed,
                              override=True)
    node = TestNode(new_node.name,
                    config_helper=config_helper,
                    config=tconf,
                    ha=nodeHa, cliha=nodeCHa, pluginPaths=allPluginsPath)
    looper.add(node)
    txnPoolNodeSet[-1] = node

    # 4. Make sure the pool is functional and all nodes have equal data
    looper.run(checkNodesConnected(txnPoolNodeSet))
    waitNodeDataEquality(looper, node, *txnPoolNodeSet[:-1],
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    sdk_ensure_pool_functional(looper, txnPoolNodeSet, new_steward_wallet, sdk_pool_handle)

    # 5. Make sure that no additional view changes happened
    assert all(n.viewNo == orig_view_no for n in txnPoolNodeSet)


def testNodeInitRemoteKeysErrorsNotSuppressed(looper, txnPoolNodeSet,
                                              sdk_node_theta_added,
                                              monkeypatch,
                                              sdk_pool_handle):
    TEST_EXCEPTION_MESSAGE = 'Failed to create some cert files'

    new_steward_wallet, new_node = sdk_node_theta_added

    new_node.stop()
    looper.removeProdable(name=new_node.name)
    nodeHa, nodeCHa = HA(*new_node.nodestack.ha), HA(*new_node.clientstack.ha)
    sigseed = randomString(32).encode()
    verkey = base58.b58encode(SimpleSigner(seed=sigseed).naclSigner.verraw).decode("utf-8")

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

    sdk_change_node_keys(looper, new_node, new_steward_wallet, sdk_pool_handle, verkey)

    monkeypatch.undo()
