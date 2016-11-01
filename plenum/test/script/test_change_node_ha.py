import pytest
from plenum.common.looper import Looper
from plenum.common.port_dispenser import genHa
from plenum.common.script_helper import changeHA
from plenum.common.signer_simple import SimpleSigner
from plenum.common.types import HA

from plenum.common.util import getMaxFailures
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd, TestNode, checkNodesConnected, genTestClient


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


def testChangeNodeHa(looper, txnPoolNodeSet, tdirWithPoolTxns,
                     tdir, poolTxnData, poolTxnStewardData, tconf):
    subjectedNode = txnPoolNodeSet[0]
    nodeSeed = poolTxnData["seeds"][subjectedNode.name].encode()
    stewardName, stewardsSeed = poolTxnStewardData
    ip, port = genHa()
    nodeStackNewHA = HA(ip, port)
    clientStackNewHA = HA(ip, port + 1)

    # stop node which HA will be changed
    subjectedNode.stop()
    # change HA
    client, req = changeHA(looper, tconf, subjectedNode.name, nodeSeed,
                           nodeStackNewHA, stewardName, stewardsSeed)
    f = getMaxFailures(len(client.nodeReg))
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId,
                          f, retryWait=1, timeout=8))
    # start node with new HA
    restartedNode = TestNode(subjectedNode.name, basedirpath=tdirWithPoolTxns,
                             config=tconf, ha=nodeStackNewHA,
                             cliha=clientStackNewHA)
    looper.add(restartedNode)
    looper.run(checkNodesConnected(txnPoolNodeSet))

    # start client and check the node HA
    anotherClient, _ = genTestClient(tmpdir=tdir, usePoolLedger=True)
    looper.add(anotherClient)
    looper.run(anotherClient.ensureConnectedToNodes())

    pass
    # TODO: Once it is sure, that node ha is changed, following is pending
    # 1. Restart nodes with new node ha
    # 2. Start a new client (should have different tdir)
    # with pool txn files created there, and have it connect to those nodes
    # 3. Check that client's master pool txn file
    # gets updated (corresponding code needs to be written)
    # 4. Any other tests we can think of to thoroughly test it


def testStopScriptIfNodeIsRunning(looper, txnPoolNodeSet, poolTxnData,
                                  poolTxnStewardData, tconf):
    nodeName = txnPoolNodeSet[0].name
    nodeSeed = poolTxnData["seeds"][nodeName].encode()
    stewardName, stewardsSeed = poolTxnStewardData
    ip, port = genHa()
    nodeStackNewHA = HA(ip, port)

    # the node `nodeName` is not stopped here

    # change HA
    with pytest.raises(Exception, message="Node '{}' must be stopped "
                                          "before".format(nodeName)):
        changeHA(looper, tconf, nodeName, nodeSeed, nodeStackNewHA,
                 stewardName, stewardsSeed)
