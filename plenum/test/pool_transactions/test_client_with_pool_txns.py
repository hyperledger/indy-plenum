import pytest

from plenum.common.log import getlogger
from plenum.common.looper import Looper
from plenum.common.util import randomString
from plenum.test.helper import genTestClient
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame


logger = getlogger()


@pytest.yield_fixture(scope="module")
def looper():
    with Looper() as l:
        yield l


def testClientUsingPoolTxns(looper, txnPoolNodeSet, poolTxnClient):
    """
    Client should not be using node registry but pool transaction file
    :return:
    """
    client, wallet = poolTxnClient
    looper.add(client)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet)


def testClientConnectAfterRestart(looper, txnPoolNodeSet, tdirWithPoolTxns):
    cname = "testClient" + randomString(5)
    newClient, _ = genTestClient(tmpdir=tdirWithPoolTxns, name=cname,
                                 usePoolLedger=True)
    logger.debug("{} starting at {}".format(newClient, newClient.nodestack.ha))
    looper.add(newClient)
    logger.debug("Public keys of client {} {}".format(
        newClient.nodestack.local.priver.keyhex,
        newClient.nodestack.local.priver.pubhex))
    logger.debug("Signer keys of client {} {}".format(
        newClient.nodestack.local.signer.keyhex,
        newClient.nodestack.local.signer.verhex))
    looper.run(newClient.ensureConnectedToNodes())
    newClient.stop()
    looper.removeProdable(newClient)
    newClient, _ = genTestClient(tmpdir=tdirWithPoolTxns, name=cname,
                                 usePoolLedger=True)
    logger.debug("{} again starting at {}".format(newClient,
                                                  newClient.nodestack.ha))
    looper.add(newClient)
    logger.debug("Public keys of client {} {}".format(
        newClient.nodestack.local.priver.keyhex,
        newClient.nodestack.local.priver.pubhex))
    logger.debug("Signer keys of client {} {}".format(
        newClient.nodestack.local.signer.keyhex,
        newClient.nodestack.local.signer.verhex))
    looper.run(newClient.ensureConnectedToNodes())
