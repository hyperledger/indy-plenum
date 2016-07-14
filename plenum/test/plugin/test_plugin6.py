from uuid import uuid4

import pytest

from plenum.common.txn import TXN_TYPE, DATA
from plenum.test.eventually import eventually
from plenum.test.helper import TestNodeSet, setupClients, TestClient, \
    checkSufficientRepliesRecvd
from plenum.test.plugin.helper import pluginPath
from plenum.test.plugin.plugin6.plugin_auction_req_processor import \
    AUCTION_START, ID, AUCTION_END


@pytest.fixture(scope="module")
def pluginVerPath():
    return pluginPath("plugin5")


@pytest.fixture(scope="module")
def pluginPrcPath():
    return pluginPath("plugin6")


@pytest.yield_fixture(scope="module")
def nodeSet(tdir, nodeReg, pluginVerPath, pluginPrcPath):
    """
    Overrides the fixture from conftest.py
    """
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir,
                     opVerificationPluginPath=pluginVerPath,
                     reqProcessorPluginPath=pluginPrcPath) as ns:

        for n in ns:  # type: Node
            assert n.reqProcessors is not None
            assert len(n.reqProcessors) == 1
            reqProcessor, = n.reqProcessors
            assert reqProcessor.count == 0
        yield ns


@pytest.fixture(scope="module")
def clients(looper, nodeSet, tdir):
    return setupClients(5, looper, nodeSet, tmpdir=tdir)


def auction(looper, client: TestClient, aucId, start=True):
    req, = client.submit({
        TXN_TYPE: AUCTION_START if start else AUCTION_END,
        DATA: {
            ID: aucId
        }
    })
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId,
                          1, retryWait=1, timeout=10))
    return req


def testAuctionTransactions(nodeSet, up, looper, clients):
    jason, tyler, les, john, timothy = tuple(clients.keys())
    auctionId = str(uuid4())
    # Jason creating auction
    saReqJl = auction(looper, clients[jason], auctionId, start=True)

