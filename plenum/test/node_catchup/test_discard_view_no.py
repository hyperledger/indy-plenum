import time
from functools import partial

from plenum.common.types import Nomination, PrePrepare
from plenum.common.util import randomString
from plenum.test.eventually import eventually
from plenum.test.helper import delayNonPrimaries, \
    sendReqsToNodesAndVerifySuffReplies, checkViewNoForNodes, \
    checkNodesConnected, checkDiscardMsg, checkProtocolInstanceSetup
from plenum.test.node_catchup.helper import checkNodeLedgersForEquality
from plenum.test.pool_transactions.helper import addNewStewardAndNode


def testNodeDiscardMessageFromUnknownView(txnPoolNodeSet,
                                          nodeSetWithNodeAddedAfterSomeTxns,
                                          newNodeCaughtUp, tdirWithPoolTxns,
                                          txnPoolCliNodeReg, tconf,
                                          allPluginsPath):
    """
    Node discards 3-phase and election messages from view nos that it does not
    know of (view nos before it joined the pool)
    :return:
    """
    looper, nodeX, _, client = nodeSetWithNodeAddedAfterSomeTxns
    viewNo = nodeX.viewNo

    # Delay processing of PRE-PREPARE from all non primary replicas of master
    # so master's performance falls and view changes
    delayNonPrimaries(txnPoolNodeSet, 0, 10)
    sendReqsToNodesAndVerifySuffReplies(looper, client, 4)
    looper.run(eventually(partial(checkViewNoForNodes, txnPoolNodeSet, viewNo + 1),
                          retryWait=1, timeout=20))

    newStewardName = "testClientSteward" + randomString(3)
    nodeName = "Theta"
    _, nodeTheta = addNewStewardAndNode(looper, client,
                                               newStewardName,
                                               nodeName,
                                               txnPoolCliNodeReg,
                                               tdirWithPoolTxns, tconf,
                                               allPluginsPath)
    txnPoolNodeSet.append(nodeTheta)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=5))
    looper.run(client.ensureConnectedToNodes())
    looper.run(eventually(checkNodeLedgersForEquality, nodeTheta,
                          *txnPoolNodeSet[:-1], retryWait=1, timeout=5))
    checkProtocolInstanceSetup(looper, txnPoolNodeSet, retryWait=1,
                               timeout=10)
    electMsg = Nomination(nodeX.name, 0, viewNo)
    threePMsg = PrePrepare(
            0,
            viewNo,
            10,
            client.defaultIdentifier,
            client.lastReqId+1,
            "random digest",
            time.time()
            )
    ridTheta = nodeX.nodestack.getRemote(nodeTheta.name).uid
    nodeX.send(electMsg, ridTheta)
    nodeX.send(threePMsg, ridTheta)
    nodeX.send(electMsg, ridTheta)
    looper.run(eventually(checkDiscardMsg, [nodeTheta, ], electMsg,
                          'un-acceptable viewNo', retryWait=1, timeout=5))
    nodeX.send(threePMsg, ridTheta)
    looper.run(eventually(checkDiscardMsg, [nodeTheta, ], threePMsg,
                          'un-acceptable viewNo', retryWait=1, timeout=5))