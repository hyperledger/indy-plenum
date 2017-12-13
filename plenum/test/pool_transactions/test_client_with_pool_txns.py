from stp_core.loop.eventually import eventually
from stp_core.common.log import getlogger
from plenum.common.util import randomString, bootstrapClientKeys
from plenum.test import waits
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies, \
    sendRandomRequest, waitForSufficientRepliesForRequests
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame
from plenum.test.test_client import genTestClient
from plenum.test.test_node import checkNodesConnected, TestNode, \
    ensureElectionsDone
from plenum.common.config_helper import PNodeConfigHelper

logger = getlogger()


def testClientUsingPoolTxns(looper, txnPoolNodeSet, poolTxnClient):
    """
    Client should not be using node registry but pool transaction file
    :return:
    """
    client, wallet = poolTxnClient
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet)


def testClientConnectAfterRestart(looper, txnPoolNodeSet, tdirWithClientPoolTxns):
    cname = "testClient" + randomString(5)
    newClient, _ = genTestClient(tmpdir=tdirWithClientPoolTxns, name=cname,
                                 usePoolLedger=True)
    logger.debug("{} starting at {}".format(newClient, newClient.nodestack.ha))
    looper.add(newClient)
    logger.debug("Public keys of client {} {}".format(
        newClient.nodestack.prihex,
        newClient.nodestack.pubhex))
    logger.debug("Signer keys of client {} {}".format(
        newClient.nodestack.keyhex,
        newClient.nodestack.verhex))
    looper.run(newClient.ensureConnectedToNodes())
    newClient.stop()
    looper.removeProdable(newClient)
    newClient, _ = genTestClient(tmpdir=tdirWithClientPoolTxns, name=cname,
                                 usePoolLedger=True)
    logger.debug("{} again starting at {}".format(newClient,
                                                  newClient.nodestack.ha))
    looper.add(newClient)
    logger.debug("Public keys of client {} {}".format(
        newClient.nodestack.prihex,
        newClient.nodestack.pubhex))
    logger.debug("Signer keys of client {} {}".format(
        newClient.nodestack.keyhex,
        newClient.nodestack.verhex))
    looper.run(newClient.ensureConnectedToNodes())


def testClientConnectToRestartedNodes(looper, txnPoolNodeSet, tdirWithPoolTxns,
                                      tdir, tdirWithClientPoolTxns,
                                      poolTxnClientNames, poolTxnData, tconf,
                                      poolTxnNodeNames, allPluginsPath):
    name = poolTxnClientNames[-1]
    newClient, w = genTestClient(tmpdir=tdirWithClientPoolTxns, nodes=txnPoolNodeSet,
                                 name=name, usePoolLedger=True)
    looper.add(newClient)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newClient,
                                                  *txnPoolNodeSet)
    sendReqsToNodesAndVerifySuffReplies(looper, w, newClient, 1, 1)
    for node in txnPoolNodeSet:
        node.stop()
        looper.removeProdable(node)

    # looper.run(newClient.ensureDisconnectedToNodes(timeout=60))
    txnPoolNodeSet = []
    for nm in poolTxnNodeNames:
        config_helper = PNodeConfigHelper(nm, tconf, chroot=tdir)
        node = TestNode(nm,
                        config_helper=config_helper,
                        config=tconf, pluginPaths=allPluginsPath)
        looper.add(node)
        txnPoolNodeSet.append(node)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    def chk():
        for node in txnPoolNodeSet:
            assert node.isParticipating

    timeout = waits.expectedPoolGetReadyTimeout(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))

    bootstrapClientKeys(w.defaultId, w.getVerkey(), txnPoolNodeSet)

    req = sendRandomRequest(w, newClient)
    waitForSufficientRepliesForRequests(looper, newClient, requests=[req])
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, newClient,
                                                  *txnPoolNodeSet)

    sendReqsToNodesAndVerifySuffReplies(looper, w, newClient, 3, 1)
