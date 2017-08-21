from functools import partial
from uuid import uuid4

import pytest

from stp_core.loop.eventually import eventuallyAll
from plenum.common.constants import TXN_TYPE, DATA
from plenum.common.types import PLUGIN_TYPE_VERIFICATION
from plenum.server.node import Node
from plenum.server.plugin_loader import PluginLoader
from plenum.test import waits
from plenum.test.helper import checkReqNack
from plenum.test.plugin.auction_req_validation.plugin_auction_req_validation \
    import AMOUNT, PLACE_BID, AUCTION_START, ID, AUCTION_END
from plenum.test.plugin.conftest import AUCTION_REQ_VALIDATION_PLUGIN_PATH_VALUE
from plenum.test.plugin.helper import getPluginPath, submitOp, makeReason
from plenum.test.test_node import TestNodeSet


@pytest.fixture(scope="module")
def pluginVerPath():
    return getPluginPath(AUCTION_REQ_VALIDATION_PLUGIN_PATH_VALUE)


@pytest.fixture(scope="module")
def allPluginPaths(pluginVerPath):
    return [pluginVerPath]


@pytest.yield_fixture(scope="module")
def nodeSet(tdir, nodeReg, allPluginPaths):
    """
    Overrides the fixture from conftest.py
    """
    with TestNodeSet(nodeReg=nodeReg,
                     tmpdir=tdir,
                     pluginPaths=allPluginPaths) as ns:

        for n in ns:  # type: Node
            assert n.opVerifiers is not None
            assert len(n.opVerifiers) == 1
            opVerifier, = n.opVerifiers
            assert opVerifier.count == 0

        yield ns


def testAuctionReqValidationPlugin(looper, nodeSet, wallet1, client1, tdir,
                                   pluginVerPath):
    # TODO: Test more cases
    plugin = PluginLoader(pluginVerPath)
    plugin = next(iter(plugin.plugins[PLUGIN_TYPE_VERIFICATION]))
    commonError = "client request invalid: InvalidClientRequest()"
    allCoros = []
    op = {
        TXN_TYPE: "dummy",
        DATA: {
            AMOUNT: 30
        }}
    req = submitOp(wallet1, client1, op)
    validTypes = ', '.join(plugin.validTxnTypes)
    update = {
        'reason': makeReason(commonError, "dummy is not a valid transaction "
                                          "type, must be one of {}"
                             .format(validTypes))}

    allCoros += [partial(checkReqNack, client1, node, req.identifier,
                         req.reqId, update) for node in nodeSet]

    op = {
        TXN_TYPE: AUCTION_START,
    }
    req = submitOp(wallet1, client1, op)
    update = {
        'reason': makeReason(commonError,
                             "{} attribute is missing or not in proper format"
                             .format(DATA))}

    allCoros += [partial(checkReqNack, client1, node, req.identifier,
                         req.reqId, update) for node in nodeSet]

    op = {
        TXN_TYPE: PLACE_BID,
    }
    req = submitOp(wallet1, client1, op)
    update = {
        'reason': makeReason(commonError,
                             "{} attribute is missing or not in proper format"
                             .format(DATA))}

    allCoros += [partial(checkReqNack, client1, node, req.identifier,
                         req.reqId, update) for node in nodeSet]

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: "some string"
    }
    req = submitOp(wallet1, client1, op)
    update = {
        'reason': makeReason(commonError,
                             "{} attribute is missing or not in proper format"
                             .format(DATA))}

    allCoros += [partial(checkReqNack, client1, node, req.identifier,
                         req.reqId, update) for node in nodeSet]

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {
            AMOUNT: 453
        }}
    req = submitOp(wallet1, client1, op)
    update = {
        'reason': makeReason(commonError, "No id provided for auction")}

    allCoros += [partial(checkReqNack, client1, node, req.identifier,
                         req.reqId, update) for node in nodeSet]

    op = {
        TXN_TYPE: AUCTION_START,
        DATA: {}
    }
    req = submitOp(wallet1, client1, op)
    update = {
        'reason': makeReason(commonError, "No id provided for auction")}

    allCoros += [partial(checkReqNack, client1, node, req.identifier,
                         req.reqId, update) for node in nodeSet]

    op = {
        TXN_TYPE: AUCTION_END,
        DATA: {}
    }
    req = submitOp(wallet1, client1, op)
    update = {
        'reason': makeReason(commonError, "No id provided for auction")}

    allCoros += [partial(checkReqNack, client1, node, req.identifier,
                         req.reqId, update) for node in nodeSet]

    auctionId = str(uuid4())

    op = {
        TXN_TYPE: PLACE_BID,
        DATA: {
            ID: auctionId,
            AMOUNT: -3
        }
    }
    req = submitOp(wallet1, client1, op)
    update = {
        'reason': makeReason(commonError, "{} must be present and should be "
                                          "a number greater than 0"
                             .format(AMOUNT))}

    allCoros += [partial(checkReqNack, client1, node, req.identifier,
                         req.reqId, update) for node in nodeSet]
    timeout = waits.expectedReqAckQuorumTime()
    looper.run(eventuallyAll(*allCoros, totalTimeout=timeout))

    for n in nodeSet:  # type: Node
        opVerifier, = n.opVerifiers
        assert opVerifier.count == 0
