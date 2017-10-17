
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


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


def changeNodeHa(looper, txnPoolNodeSet, tdirWithPoolTxns,
                 poolTxnData, poolTxnStewardNames, tconf, shouldBePrimary, tdir):

    # prepare new ha for node and client stack
    subjectedNode = None
    stewardName = None
    stewardsSeed = None

    for nodeIndex, n in enumerate(txnPoolNodeSet):
        if shouldBePrimary == n.has_master_primary:
            subjectedNode = n
            stewardName = poolTxnStewardNames[nodeIndex]
            stewardsSeed = poolTxnData["seeds"][stewardName].encode()
            break

    nodeStackNewHA, clientStackNewHA = genHa(2)
    logger.debug("change HA for node: {} to {}". format(
        subjectedNode.name, (nodeStackNewHA, clientStackNewHA)))

    nodeSeed = poolTxnData["seeds"][subjectedNode.name].encode()

    # change HA
    stewardClient, req = changeHA(looper, tconf, subjectedNode.name, nodeSeed,
                                  nodeStackNewHA, stewardName, stewardsSeed, basedir=tdir)

    waitForSufficientRepliesForRequests(looper, stewardClient,
                                        requests=[req])

    # stop node for which HA will be changed
    subjectedNode.stop()
    looper.removeProdable(subjectedNode)

    # start node with new HA
    restartedNode = TestNode(subjectedNode.name, basedirpath=tdirWithPoolTxns, base_data_dir=tdirWithPoolTxns,
                             config=tconf, ha=nodeStackNewHA,
                             cliha=clientStackNewHA)
    looper.add(restartedNode)
    txnPoolNodeSet[nodeIndex] = restartedNode
    looper.run(checkNodesConnected(txnPoolNodeSet, customTimeout=70))

    electionTimeout = waits.expectedPoolElectionTimeout(
        nodeCount=len(txnPoolNodeSet),
        numOfReelections=3)
    ensureElectionsDone(looper,
                        txnPoolNodeSet,
                        retryWait=1,
                        customTimeout=electionTimeout)

    # start client and check the node HA
    anotherClient, _ = genTestClient(tmpdir=tdirWithPoolTxns,
                                     usePoolLedger=True)
    looper.add(anotherClient)
    looper.run(eventually(anotherClient.ensureConnectedToNodes))
    stewardWallet = Wallet(stewardName)
    stewardWallet.addIdentifier(signer=SimpleSigner(seed=stewardsSeed))
    sendReqsToNodesAndVerifySuffReplies(
        looper, stewardWallet, stewardClient, 8)
