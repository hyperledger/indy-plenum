from uuid import uuid4

import pytest

from plenum.common.constants import TXN_TYPE, DATA, TARGET_NYM
from plenum.test.helper import setupClients, \
    waitForSufficientRepliesForRequests
from plenum.test.plugin.auction_req_processor.plugin_auction_req_processor import \
    AUCTION_START, ID, AUCTION_END, GET_BAL, BALANCE, PLACE_BID, AMOUNT
from plenum.test.plugin.conftest import AUCTION_REQ_VALIDATION_PLUGIN_PATH_VALUE, \
    AUCTION_REQ_PROCESSOR_PLUGIN_PATH_VALUE
from plenum.test.plugin.helper import getPluginPath, App
from plenum.test.test_node import TestNodeSet


@pytest.fixture(scope="module")
def pluginVerPath():
    return getPluginPath(AUCTION_REQ_VALIDATION_PLUGIN_PATH_VALUE)


@pytest.fixture(scope="module")
def pluginPrcPath():
    return getPluginPath(AUCTION_REQ_PROCESSOR_PLUGIN_PATH_VALUE)


@pytest.fixture(scope="module")
def allPluginPaths(pluginVerPath, pluginPrcPath):
    return [pluginVerPath, pluginPrcPath]


@pytest.yield_fixture(scope="module")
def nodeSet(tdir, nodeReg, allPluginPaths):
    """
    Overrides the fixture from conftest.py
    """
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir,
                     pluginPaths=allPluginPaths
                     ) as ns:

        for n in ns:  # type: Node
            assert n.reqProcessors is not None
            assert len(n.reqProcessors) == 1
            reqProcessor, = n.reqProcessors
            assert reqProcessor.count == 0
        yield ns


@pytest.fixture(scope="module")
def apps(looper, nodeSet, tdir):
    cs, ws = setupClients(5, looper, nodeSet, tmpdir=tdir)
    return [AuctionApp(ws[k], cs[k], looper) for k in cs.keys()]


class AuctionApp(App):

    def auction(self, aucId, start=True):
        req = self.submit({
            TXN_TYPE: AUCTION_START if start else AUCTION_END,
            DATA: {
                ID: aucId
            }
        })
        waitForSufficientRepliesForRequests(self.looper, self.client,
                                            requests=[req])
        return req

    def getBalance(self) -> int:
        req = self.submit({
            TXN_TYPE: GET_BAL,
            TARGET_NYM: self.wallet.defaultId
        })
        waitForSufficientRepliesForRequests(self.looper, self.client,
                                            requests=[req])
        return self.client.hasConsensus(*req.key)[BALANCE]

    def bid(self, aucId, amount):
        req = self.submit({
            TXN_TYPE: PLACE_BID,
            DATA: {
                ID: aucId,
                AMOUNT: amount
            }
        })
        waitForSufficientRepliesForRequests(self.looper, self.client,
                                            requests=[req])
        return req


def testAuctionTransactions(nodeSet, up, looper, apps):
    jason, tyler, les, john, timothy = apps
    auctionId = str(uuid4())
    bal1Ty = tyler.getBalance()
    bal1Le = les.getBalance()
    bal1Jn = john.getBalance()
    bal1Ti = timothy.getBalance()

    # Jason creating auction
    saReqJl = jason.auction(auctionId, start=True)

    # Tyler bidding 20
    biReqTy = tyler.bid(auctionId, 20)

    # Tyler's balance should be 20 less than his starting balance
    bal2Ty = tyler.getBalance()
    assert bal1Ty - bal2Ty == 20

    # Les bidding 40
    biReqLe = les.bid(auctionId, 40)

    # Tyler should get back his money and his balance should be same as starting
    #  balance
    bal3Ty = tyler.getBalance()
    assert bal3Ty == bal1Ty

    # Les's balance should be 40 less than his starting balance
    bal2Le = les.getBalance()
    assert bal1Le - bal2Le == 40

    # John bids 30 but since that is lower than the highest bid, it is rejected
    biReqJn = john.bid(auctionId, 30)
    bal2Jn = john.getBalance()
    assert bal2Jn == bal1Jn

    # Timothy bids 200 and the highest bidder, i.e Les' money should be returned
    #  and Timothy's balance shoulbe 200 less than the starting balance
    biReqTi = timothy.bid(auctionId, 200)
    bal2Ti = timothy.getBalance()
    assert bal1Ti - bal2Ti == 200
    bal3Le = les.getBalance()
    assert bal3Le == bal1Le

    # Jason ending auction
    enReqJl = jason.auction(auctionId, start=False)

    # John bids 300 which is higher than the highest bid but the auction has
    # ended so his money is not deducted
    biReqJn = john.bid(auctionId, 300)
    bal3Jn = john.getBalance()
    assert bal3Jn == bal1Jn

    # Every one's balance should be same as their initial balance except
    # Timothy's balance
    bal4Ty = tyler.getBalance()
    bal4Le = les.getBalance()
    bal4Jn = john.getBalance()
    bal3Ti = timothy.getBalance()
    assert bal4Ty == bal1Ty
    assert bal4Le == bal1Le
    assert bal4Jn == bal3Jn
    assert bal1Ti - bal3Ti == 200
