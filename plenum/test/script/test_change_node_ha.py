import os

import pytest
from plenum.common.constants import ENVS
from plenum.common.looper import Looper
from plenum.common.port_dispenser import genHa
from plenum.common.script_helper import changeHA
from plenum.common.signer_simple import SimpleSigner
from plenum.common.types import HA

from plenum.common.util import getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd, TestNode, checkNodesConnected, genTestClient, \
    ensureElectionsDone


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


whitelist = ['found legacy entry', "doesn't match", "reconciling nodeReg",
             "missing", "conflicts", "matches", "nodeReg", "conflicting address"]


def changeNodeHa(looper, txnPoolNodeSet, tdirWithPoolTxns, tdir,
                 poolTxnData, poolTxnStewardNames, tconf, shouldBePrimary):

    # prepare new ha for node and client stack
    subjectedNode = None
    stewardName = None
    stewardsSeed = None

    for nodeIndex, n in enumerate(txnPoolNodeSet):
        # TODO: Following condition is not correct to
        # identify primary (as primaryReplicaNo is None),
        # need to add proper condition accordingly
        if (shouldBePrimary and n.primaryReplicaNo == 0) or \
                (not shouldBePrimary and n.primaryReplicaNo != 0):
            subjectedNode = n
            stewardName = poolTxnStewardNames[nodeIndex]
            stewardsSeed = poolTxnData["seeds"][stewardName].encode()
            break

    print("change HA for node: {}".format(subjectedNode.name))
    nodeSeed = poolTxnData["seeds"][subjectedNode.name].encode()

    # stewardName, stewardsSeed = poolTxnStewardData
    nodeStackNewHA, clientStackNewHA = genHa(2)

    # change HA
    client, req = changeHA(looper, tconf, subjectedNode.name, nodeSeed,
                           nodeStackNewHA, stewardName, stewardsSeed)
    f = getMaxFailures(len(client.nodeReg))
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId,
                          f, retryWait=1, timeout=15))

    # stop node for which HA will be changed
    subjectedNode.stop()

    # start node with new HA
    restartedNode = TestNode(subjectedNode.name, basedirpath=tdirWithPoolTxns,
                             config=tconf, ha=nodeStackNewHA,
                             cliha=clientStackNewHA)
    # txnPoolNodeSet[0] = restartedNode
    looper.add(restartedNode)
    txnPoolNodeSet[nodeIndex] = restartedNode
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet))
    ensureElectionsDone(looper, txnPoolNodeSet, retryWait=1, timeout=10)

    # start client and check the node HA
    anotherClient, _ = genTestClient(tmpdir=tdir, usePoolLedger=True)
    looper.add(anotherClient)
    looper.run(eventually(anotherClient.ensureConnectedToNodes))

    baseDirs = set()
    for n in txnPoolNodeSet:
        baseDirs.add(n.config.baseDir)
    baseDirs.add(client.config.baseDir)
    baseDirs.add(anotherClient.config.baseDir)

    for baseDir in baseDirs:
        for name, env in ENVS.items():
            poolLedgerPath = os.path.join(baseDir, env.poolLedger)
            if os.path.exists(poolLedgerPath):
                with open(poolLedgerPath) as f:
                    poolLedgerContent = f.read()
                    print("#### pool ledger content: \n{}".format(poolLedgerContent))
                    assert nodeStackNewHA.host in poolLedgerContent
                    assert str(nodeStackNewHA.port) in poolLedgerContent
                    assert clientStackNewHA.host in poolLedgerContent
                    assert str(clientStackNewHA.port) in poolLedgerContent


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


# TODO: Following needs to be tested yet
# def testChangeNodeHaForPrimary(looper, txnPoolNodeSet, tdirWithPoolTxns,
#                      tdir, poolTxnData, poolTxnStewardNames, tconf):
#     changeNodeHa(looper, txnPoolNodeSet, tdirWithPoolTxns, tdir,
#                  poolTxnData, poolTxnStewardNames, tconf, shouldBePrimary=True)
#


def testChangeNodeHaForNonPrimary(looper, txnPoolNodeSet, tdirWithPoolTxns,
                                  tdir, poolTxnData, poolTxnStewardNames, tconf):
    changeNodeHa(looper, txnPoolNodeSet, tdirWithPoolTxns, tdir,
                 poolTxnData, poolTxnStewardNames, tconf, shouldBePrimary=True)


