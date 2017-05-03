import filecmp
import os

import pytest

from plenum.client.wallet import Wallet
from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.script_helper import changeHA
from plenum.common.signer_simple import SimpleSigner
from plenum.common.util import getMaxFailures
from plenum.test import waits
from plenum.test.helper import waitForSufficientRepliesForRequests, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_client import genTestClient
from plenum.test.test_node import TestNode, checkNodesConnected, \
    ensureElectionsDone
from stp_core.network.port_dispenser import genHa

logger = getlogger()


@pytest.fixture(scope="module")
def tconf(tconf, request):
    oldVal = tconf.UpdateGenesisPoolTxnFile
    tconf.UpdateGenesisPoolTxnFile = True

    def reset():
        tconf.UpdateGenesisPoolTxnFile = oldVal

    request.addfinalizer(reset)
    return tconf


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


def checkIfGenesisPoolTxnFileUpdated(*nodesAndClients):
    for item in nodesAndClients:
        poolTxnFileName = item.poolManager.ledgerFile if \
            isinstance(item, TestNode) else item.ledgerFile
        genFile = os.path.join(item.basedirpath, poolTxnFileName)
        ledgerFile = os.path.join(item.dataLocation, poolTxnFileName)
        assert filecmp.cmp(genFile, ledgerFile, shallow=False)


def changeNodeHa(looper, txnPoolNodeSet, tdirWithPoolTxns,
                 poolTxnData, poolTxnStewardNames, tconf, shouldBePrimary):

    # prepare new ha for node and client stack
    subjectedNode = None
    stewardName = None
    stewardsSeed = None

    for nodeIndex, n in enumerate(txnPoolNodeSet):
        if (shouldBePrimary and n.primaryReplicaNo == 0) or \
                (not shouldBePrimary and n.primaryReplicaNo != 0):
            subjectedNode = n
            stewardName = poolTxnStewardNames[nodeIndex]
            stewardsSeed = poolTxnData["seeds"][stewardName].encode()
            break

    nodeStackNewHA, clientStackNewHA = genHa(2)
    logger.debug("change HA for node: {} to {}".
                 format(subjectedNode.name, (nodeStackNewHA, clientStackNewHA)))

    nodeSeed = poolTxnData["seeds"][subjectedNode.name].encode()

    # change HA
    stewardClient, req = changeHA(looper, tconf, subjectedNode.name, nodeSeed,
                                  nodeStackNewHA, stewardName, stewardsSeed)

    logger.info("PointX - A")
    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req])
    logger.info("PointX - B")


    # stop node for which HA will be changed
    subjectedNode.stop()
    logger.info("PointX - C")
    looper.removeProdable(subjectedNode)
    logger.info("PointX - D")

    # start node with new HA
    restartedNode = TestNode(subjectedNode.name, basedirpath=tdirWithPoolTxns,
                             config=tconf, ha=nodeStackNewHA,
                             cliha=clientStackNewHA)
    logger.info("PointX - E")
    looper.add(restartedNode)
    logger.info("PointX - F")

    txnPoolNodeSet[nodeIndex] = restartedNode

    logger.info("PointX - G")
    looper.run(checkNodesConnected(txnPoolNodeSet, customTimeout=70))
    logger.info("PointX - H")
    electionTimeout = \
        waits.expectedPoolElectionTimeout(
            nodeCount=len(txnPoolNodeSet),
            numOfReelections=3)
    ensureElectionsDone(looper,
                        txnPoolNodeSet,
                        retryWait=1,
                        customTimeout=electionTimeout)
    logger.info("PointX - I")

    # start client and check the node HA
    anotherClient, _ = genTestClient(tmpdir=tdirWithPoolTxns,
                                     usePoolLedger=True)
    logger.info("PointX - J")
    looper.add(anotherClient)
    logger.info("PointX - K")
    looper.run(eventually(anotherClient.ensureConnectedToNodes))
    logger.info("PointX - L")
    stewardWallet = Wallet(stewardName)
    logger.info("PointX - M")
    stewardWallet.addIdentifier(signer=SimpleSigner(seed=stewardsSeed))
    logger.info("PointX - N")
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, stewardClient, 8)
    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet) + 1)
    logger.info("PointX - O")
    if tconf.UpdateGenesisPoolTxnFile:
        looper.run(eventually(checkIfGenesisPoolTxnFileUpdated, *txnPoolNodeSet,
                              stewardClient, anotherClient, retryWait=1,
                              timeout=timeout))
    logger.info("PointX - P")
