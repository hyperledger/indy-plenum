import os
from functools import partial

import pytest

from plenum.common.txn import TXN_TYPE, TARGET_NYM, DATA
from plenum.common.types import PLUGIN_TYPE_VERIFICATION
from plenum.server.node import Node
from plenum.server.plugin_loader import PluginLoader
from plenum.test.eventually import eventuallyAll, eventually
from plenum.test.helper import setupClient, \
    checkReqNack, checkSufficientRepliesRecvd
from plenum.test.test_node import TestNodeSet
from plenum.test.plugin.bank_req_validation.plugin_bank_req_validation import \
    AMOUNT, CREDIT
from plenum.test.plugin.conftest import BANK_REQ_VALIDATION_PLUGIN_PATH_VALUE
from plenum.test.plugin.helper import getPluginPath, submitOp, makeReason


@pytest.fixture(scope="module")
def pluginVerPath():
    return getPluginPath(BANK_REQ_VALIDATION_PLUGIN_PATH_VALUE)


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


def testBankReqValidationPlugin(looper, nodeSet, client1, wallet1, tdir,
                                pluginVerPath):
    plugin = PluginLoader(pluginVerPath)
    plugin = next(iter(plugin.plugins[PLUGIN_TYPE_VERIFICATION]))
    commonError = "client request invalid: InvalidClientRequest()"
    client2, wallet2 = setupClient(looper, nodeSet, tmpdir=tdir)
    req = submitOp(wallet1, client1, {
        TXN_TYPE: "dummy",
        TARGET_NYM: wallet2.defaultId,
        DATA: {
            AMOUNT: 30
        }})

    validTypes = ', '.join(plugin.validTxnTypes)
    update = {'reason': makeReason(commonError, "dummy is not a valid "
                                                "transaction type, must be "
                                                "one of {}".format(validTypes))}

    coros1 = [partial(checkReqNack, client1, node, req.identifier,
                      req.reqId, update) for node in nodeSet]

    req = submitOp(wallet1, client1, {
        TXN_TYPE: CREDIT,
        TARGET_NYM: wallet2.defaultId,
        })

    update = {
        'reason': makeReason(commonError,
                             "{} attribute is missing or not in proper format"
                             .format(DATA))}

    coros2 = [partial(checkReqNack, client1, node, req.identifier,
                      req.reqId, update) for node in nodeSet]

    req = submitOp(wallet1, client1, {
        TXN_TYPE: CREDIT,
        TARGET_NYM: wallet2.defaultId,
        DATA: "some string"})

    update = {
        'reason': makeReason(commonError,
                             "{} attribute is missing or not in proper format"
                             .format(DATA))}

    coros3 = [partial(checkReqNack, client1, node, req.identifier,
                      req.reqId, update) for node in nodeSet]

    req = submitOp(wallet1, client1, {
        TXN_TYPE: CREDIT,
        TARGET_NYM: wallet2.defaultId,
        DATA: {
            AMOUNT: -3
        }})

    update = {
        'reason': makeReason(commonError, "{} must be present and should be "
                                          "a number greater than 0"
                             .format(AMOUNT))}

    coros4 = [partial(checkReqNack, client1, node, req.identifier, req.reqId,
                      update) for node in nodeSet]

    looper.run(eventuallyAll(*(coros1+coros2+coros3+coros4), totalTimeout=5))

    req = submitOp(wallet1, client1, {
        TXN_TYPE: CREDIT,
        TARGET_NYM: wallet2.defaultId,
        DATA: {
            AMOUNT: 30
        }})
    looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    for n in nodeSet:  # type: Node
        opVerifier, = n.opVerifiers
        assert opVerifier.count == 1
