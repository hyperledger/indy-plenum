import pytest

from plenum.test.spy_helpers import getAllReturnVals
from stp_core.common.log import getlogger
from plenum.common.util import randomString
from plenum.test.conftest import getValueFromModule
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import waitNodeDataEquality, \
    check_last_3pc_master
from plenum.test.pool_transactions.helper import \
    addNewStewardAndNode, buildPoolClientAndWallet
# noinspection PyUnresolvedReferences
from plenum.test.pool_transactions.conftest import stewardAndWallet1, \
    steward1, stewardWallet, clientAndWallet1, client1, wallet1, \
    client1Connected
from plenum.test.test_client import TestClient
from plenum.test.test_node import checkNodesConnected


def whitelist():
    return ['got error while verifying message']


logger = getlogger()


@pytest.yield_fixture(scope="module")
def looper(txnPoolNodesLooper):
    yield txnPoolNodesLooper


@pytest.yield_fixture("module")
def nodeCreatedAfterSomeTxns(looper, testNodeClass, do_post_node_creation,
                             txnPoolNodeSet, tdir, tdirWithClientPoolTxns,
                             poolTxnStewardData, tconf, allPluginsPath, request):
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithClientPoolTxns,
                                              clientClass=TestClient)
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())
    txnCount = getValueFromModule(request, "txnCount", 5)
    sendReqsToNodesAndVerifySuffReplies(looper,
                                        wallet,
                                        client,
                                        txnCount)
    newStewardName = randomString()
    newNodeName = "Epsilon"
    newStewardClient, newStewardWallet, newNode = addNewStewardAndNode(
        looper, client, wallet, newStewardName, newNodeName,
        tdir, tdirWithClientPoolTxns, tconf, nodeClass=testNodeClass,
        allPluginsPath=allPluginsPath, autoStart=True,
        do_post_node_creation=do_post_node_creation)
    yield looper, newNode, client, wallet, newStewardClient, \
        newStewardWallet


@pytest.fixture("module")
def nodeSetWithNodeAddedAfterSomeTxns(
        txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    looper, newNode, client, wallet, newStewardClient, newStewardWallet = \
        nodeCreatedAfterSomeTxns
    txnPoolNodeSet.append(newNode)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    looper.run(newStewardClient.ensureConnectedToNodes())
    looper.run(client.ensureConnectedToNodes())
    return looper, newNode, client, wallet, newStewardClient, newStewardWallet


@pytest.fixture("module")
def newNodeCaughtUp(txnPoolNodeSet, nodeSetWithNodeAddedAfterSomeTxns):
    looper, newNode, _, _, _, _ = nodeSetWithNodeAddedAfterSomeTxns
    waitNodeDataEquality(looper, newNode, *txnPoolNodeSet[:4])
    check_last_3pc_master(newNode, txnPoolNodeSet[:4])

    # Check if catchup done once
    catchup_done_once = True
    for li in newNode.ledgerManager.ledgerRegistry.values():
        catchup_done_once = catchup_done_once and (li.num_txns_caught_up > 0)

    if not catchup_done_once:
        # It might be the case that node has to do catchup again, in that case
        # check the return value of `num_txns_caught_up_in_last_catchup` to be
        # greater than 0

        assert max(
            getAllReturnVals(
                newNode,
                newNode.num_txns_caught_up_in_last_catchup)) > 0

    for li in newNode.ledgerManager.ledgerRegistry.values():
        assert not li.receivedCatchUpReplies
        assert not li.recvdCatchupRepliesFrm

    return newNode


@pytest.yield_fixture("module")
def poolAfterSomeTxns(
        looper,
        txnPoolNodesLooper,
        txnPoolNodeSet,
        tdirWithClientPoolTxns,
        poolTxnStewardData,
        allPluginsPath,
        request):
    client, wallet = buildPoolClientAndWallet(poolTxnStewardData,
                                              tdirWithClientPoolTxns,
                                              clientClass=TestClient)
    looper.run(checkNodesConnected(txnPoolNodeSet))
    looper.add(client)
    looper.run(client.ensureConnectedToNodes())
    txnCount = getValueFromModule(request, "txnCount", 5)
    sendReqsToNodesAndVerifySuffReplies(txnPoolNodesLooper,
                                        wallet,
                                        client,
                                        txnCount)
    yield looper, client, wallet
