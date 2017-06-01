import time
from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.types import Nomination, PrePrepare
from plenum.common.util import randomString
from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.test.delayers import delayNonPrimaries
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    waitForViewChange, checkDiscardMsg
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

    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's performance falls and view changes

    def forceViewChange(delay):
        try:
            cancelDelays = delayNonPrimaries(txnPoolNodeSet, 0,delay)
            sendReqsToNodesAndVerifySuffReplies(looper, wallet, client, 4)
            waitForViewChange(looper, txnPoolNodeSet, expectedViewNo=viewNo+1)
        finally:
            cancelDelays()

    # TODO it's a workaround until view change related issues are resolved
    # the goal is to force view change BUT after all responses are passed
    # to client
    delay = 0.1
    numTries = 3
    while True:
        numTries -= 1
        try:
            forceViewChange(delay)
        except AssertionError:
            if numTries > 0:
                delay += 0.1
                pass
            else:
                raise
        else:
            break

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
                          nodeX.replicas[0].lastOrderedPPSeqNo)

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
