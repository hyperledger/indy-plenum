from uuid import uuid4

import pytest

from plenum.common.txn import TXN_TYPE, DATA, TARGET_NYM
from plenum.test.eventually import eventually
from plenum.test.helper import TestNodeSet, setupClients, TestClient, \
    checkSufficientRepliesRecvd
from plenum.test.plugin.helper import pluginPath
from plenum.test.plugin.plugin6.plugin_auction_req_processor import \
    AUCTION_START, ID, AUCTION_END, GET_BAL, BALANCE, PLACE_BID, AMOUNT


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


def getBalance(looper, client: TestClient) -> int:
    req, = client.submit({
        TXN_TYPE: GET_BAL,
        TARGET_NYM: client.defaultIdentifier
    })
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId,
                          1, retryWait=1, timeout=10))
    return client.hasConsensus(req.reqId)[BALANCE]


def bid(looper, client, aucId, amount):
    req, = client.submit({
        TXN_TYPE: PLACE_BID,
        DATA: {
            ID: aucId,
            AMOUNT: amount
        }
    })
    looper.run(eventually(checkSufficientRepliesRecvd, client.inBox, req.reqId,
                          1, retryWait=1, timeout=10))
    return req


def testAuctionTransactions(nodeSet, up, looper, clients):
    jason, tyler, les, john, timothy = tuple(clients.keys())
    auctionId = str(uuid4())
    bal1Ty = getBalance(looper, clients[tyler])
    bal1Le = getBalance(looper, clients[les])
    bal1Jn = getBalance(looper, clients[john])
    bal1Ti = getBalance(looper, clients[timothy])

    # Jason creating auction
    saReqJl = auction(looper, clients[jason], auctionId, start=True)

    # Tyler bidding 20
    biReqTy = bid(looper, clients[tyler], auctionId, 20)

    # Tyler's balance should be 20 less than his starting balance
    bal2Ty = getBalance(looper, clients[tyler])
    assert bal1Ty - bal2Ty == 20

    # Les bidding 40
    biReqLe = bid(looper, clients[les], auctionId, 40)

    # Tyler should get back his money and his balance should be same as starting balance
    bal3Ty = getBalance(looper, clients[tyler])
    assert bal3Ty == bal1Ty

    # Les's balance should be 40 less than his starting balance
    bal2Le = getBalance(looper, clients[les])
    assert bal1Le - bal2Le == 40

    # John bids 30 but since that is lower than the highest bid, it is rejected
    biReqJn = bid(looper, clients[john], auctionId, 30)
    bal2Jn = getBalance(looper, clients[john])
    assert bal2Jn == bal1Jn

    # Timothy bids 200 and the highest bidder, i.e Les' money should be retruned
    #  and Timothy's balance shoulbe 200 less than the starting balance
    biReqTi = bid(looper, clients[timothy], auctionId, 200)
    bal2Ti = getBalance(looper, clients[timothy])
    assert bal1Ti - bal2Ti == 200
    bal3Le = getBalance(looper, clients[les])
    assert bal3Le == bal1Le

    # Jason ending auction
    enReqJl = auction(looper, clients[jason], auctionId, start=False)

    # John bids 300 which is higher than the highest bid but the auction has ended so his money is not deducted
    biReqJn = bid(looper, clients[john], auctionId, 300)
    bal3Jn = getBalance(looper, clients[john])
    assert bal3Jn == bal1Jn

    # Every one's balance should be same as their initial balance except Timothy's balance
    bal4Ty = getBalance(looper, clients[tyler])
    bal4Le = getBalance(looper, clients[les])
    bal4Jn = getBalance(looper, clients[john])
    bal3Ti = getBalance(looper, clients[timothy])
    assert bal4Ty == bal1Ty
    assert bal4Le == bal1Le
    assert bal4Jn == bal3Jn
    assert bal1Ti - bal3Ti == 200
