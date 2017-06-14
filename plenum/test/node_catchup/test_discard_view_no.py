import time
from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.types import Nomination, PrePrepare
from plenum.common.util import randomString
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.helper import checkDiscardMsg
from plenum.test.view_change.helper import ensure_view_change
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.pool_transactions.helper import addNewStewardAndNode
from plenum.test.test_node import checkNodesConnected, \
    checkProtocolInstanceSetup, getPrimaryReplica
from plenum.test import waits


whitelist = ['found legacy entry']  # warnings

def testNodeDiscardMessageFromUnknownView(txnPoolNodeSet,
                                          nodeSetWithNodeAddedAfterSomeTxns,
                                          newNodeCaughtUp, tdirWithPoolTxns,
                                          tconf, allPluginsPath):
    """
    Node discards 3-phase and election messages from view nos that it does not
    know of (view nos before it joined the pool)
    :return:
    """
    looper, nodeX, client, wallet, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    viewNo = nodeX.viewNo

    # Force two view changes: node discards msgs which have viewNo
    # at least two less than node's. Current protocol implementation
    # needs to hold messages from the previous view as well as
    # from the current view.
    ensure_view_change(looper, txnPoolNodeSet, client, wallet)
    ensure_view_change(looper, txnPoolNodeSet, client, wallet)

    newStewardName = "testClientSteward" + randomString(3)
    nodeName = "Theta"
    _, _, nodeTheta = addNewStewardAndNode(looper, client,
                                           wallet,
                                           newStewardName,
                                           nodeName,
                                           tdirWithPoolTxns, tconf,
                                           allPluginsPath)
    txnPoolNodeSet.append(nodeTheta)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    looper.run(client.ensureConnectedToNodes())
    waitNodeDataEquality(looper, nodeTheta, *txnPoolNodeSet[:-1])
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1)
    electMsg = Nomination(nodeX.name, 0, viewNo,
                          nodeX.replicas[0].lastOrderedPPSeqNo[1])

    primaryRepl = getPrimaryReplica(txnPoolNodeSet)
    threePMsg = PrePrepare(
            0,
            viewNo,
            10,
            time.time(),
            [[wallet.defaultId, wallet._getIdData().lastReqId+1]],
            1,
            "random digest",
            DOMAIN_LEDGER_ID,
            primaryRepl.stateRootHash(DOMAIN_LEDGER_ID),
            primaryRepl.txnRootHash(DOMAIN_LEDGER_ID),
            )
    ridTheta = nodeX.nodestack.getRemote(nodeTheta.name).uid
    nodeX.send(electMsg, ridTheta)

    messageTimeout = waits.expectedNodeToNodeMessageDeliveryTime()
    looper.run(eventually(checkDiscardMsg, [nodeTheta, ], electMsg,
                          'un-acceptable viewNo',
                          retryWait=1, timeout=messageTimeout))
    nodeX.send(threePMsg, ridTheta)
    looper.run(eventually(checkDiscardMsg, [nodeTheta, ], threePMsg,
                          'un-acceptable viewNo',
                          retryWait=1, timeout=messageTimeout))
