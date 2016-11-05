import pytest

from plenum.client.wallet import Wallet
from plenum.common.exceptions import ProdableAlreadyAdded
from plenum.common.looper import Looper
from plenum.common.port_dispenser import genHa
from plenum.common.script_helper import changeHA
from plenum.common.signer_simple import SimpleSigner

from plenum.common.util import getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd, checkNodesConnected, \
    ensureElectionsDone, sendRandomRequests, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_client import genTestClient
from plenum.test.test_node import TestNode, checkNodesConnected, \
    ensureElectionsDone
from plenum.common.log import getlogger


logger = getlogger()


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


whitelist = ['found legacy entry', "doesn't match", 'reconciling nodeReg',
             'missing', 'conflicts', 'matches', 'nodeReg',
             'conflicting address', 'unable to send message']


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
    f = getMaxFailures(len(stewardClient.nodeReg))
    looper.run(eventually(checkSufficientRepliesRecvd, stewardClient.inBox,
                          req.reqId, f, retryWait=1, timeout=15))

    # stop node for which HA will be changed
    subjectedNode.stop()
    looper.removeProdable(subjectedNode)

    # start node with new HA
    restartedNode = TestNode(subjectedNode.name, basedirpath=tdirWithPoolTxns,
                             config=tconf, ha=nodeStackNewHA,
                             cliha=clientStackNewHA)
    looper.add(restartedNode)

    txnPoolNodeSet[nodeIndex] = restartedNode
    looper.run(checkNodesConnected(txnPoolNodeSet, overrideTimeout=20))
    ensureElectionsDone(looper, txnPoolNodeSet, retryWait=1, timeout=10)

    # start client and check the node HA
    anotherClient, _ = genTestClient(tmpdir=tdirWithPoolTxns,
                                     usePoolLedger=True)
    looper.add(anotherClient)
    looper.run(eventually(anotherClient.ensureConnectedToNodes))
    stewardWallet = Wallet(stewardName)
    stewardWallet.addIdentifier(signer=SimpleSigner(seed=stewardsSeed))
    sendReqsToNodesAndVerifySuffReplies(looper, stewardWallet, stewardClient, 5)
    looper.removeProdable(stewardClient)


# TODO: This is failing as of now, fix it
# def testStopScriptIfNodeIsRunning(looper, txnPoolNodeSet, poolTxnData,
#                                   poolTxnStewardData, tconf):
#     nodeName = txnPoolNodeSet[0].name
#     nodeSeed = poolTxnData["seeds"][nodeName].encode()
#     stewardName, stewardsSeed = poolTxnStewardData
#     ip, port = genHa()
#     nodeStackNewHA = HA(ip, port)
#
#     # the node `nodeName` is not stopped here
#
#     # change HA
#     with pytest.raises(Exception, message="Node '{}' must be stopped "
#                                           "before".format(nodeName)):
#         changeHA(looper, tconf, nodeName, nodeSeed, nodeStackNewHA,
#                  stewardName, stewardsSeed)


def testChangeNodeHaForPrimary(looper, txnPoolNodeSet, tdirWithPoolTxns,
                               poolTxnData, poolTxnStewardNames, tconf):
    changeNodeHa(looper, txnPoolNodeSet, tdirWithPoolTxns,
                 poolTxnData, poolTxnStewardNames, tconf, shouldBePrimary=True)


def testChangeNodeHaForNonPrimary(looper, txnPoolNodeSet, tdirWithPoolTxns,
                                  poolTxnData, poolTxnStewardNames, tconf):
    changeNodeHa(looper, txnPoolNodeSet, tdirWithPoolTxns,
                 poolTxnData, poolTxnStewardNames, tconf, shouldBePrimary=False)


