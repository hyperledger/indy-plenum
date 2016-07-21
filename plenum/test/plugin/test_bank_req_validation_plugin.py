import os
from functools import partial

import pytest

from plenum.common.txn import TXN_TYPE, TARGET_NYM, DATA
from plenum.common.types import PLUGIN_TYPE_VERIFICATION
from plenum.server.node import Node
from plenum.server.plugin_loader import PluginLoader
from plenum.test.eventually import eventuallyAll, eventually
from plenum.test.helper import TestNodeSet, genTestClient, setupClient, \
    checkReqNack, checkSufficientRepliesRecvd
from plenum.test.plugin.bank_req_validation.plugin_bank_req_validation import AMOUNT, CREDIT
from plenum.test.plugin.conftest import BANK_REQ_VALIDATION_PLUGIN_PATH_VALUE
from plenum.test.plugin.helper import pluginPath


@pytest.fixture(scope="module")
def pluginVerPath():
    return pluginPath(BANK_REQ_VALIDATION_PLUGIN_PATH_VALUE)


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


def testBankReqValidationPlugin(looper, nodeSet, client1, tdir, pluginVerPath):
    plugin = PluginLoader(pluginVerPath)
    plugin = next(iter(plugin.plugins[PLUGIN_TYPE_VERIFICATION]))
    commonError = "client request invalid: AssertionError "
    client2 = setupClient(looper, nodeSet, tmpdir=tdir)
    req, = client1.submit({
        TXN_TYPE: "dummy",
        TARGET_NYM: client2.defaultIdentifier,
        DATA: {
            AMOUNT: 30
        }})

    update = {'reason': '{}dummy is not a valid transaction type, must be one of {}'.
        format(commonError, ', '.join(plugin.validTxnTypes))}

    coros1 = [partial(checkReqNack, client1, node, req.reqId, update)
              for node in nodeSet]

    req, = client1.submit({
        TXN_TYPE: CREDIT,
        TARGET_NYM: client2.defaultIdentifier,
        })

    update = {
        'reason': "{}{} attribute is missing or not in proper format" \
            .format(commonError, DATA)}

    coros2 = [partial(checkReqNack, client1, node, req.reqId, update)
             for node in nodeSet]

    req, = client1.submit({
        TXN_TYPE: CREDIT,
        TARGET_NYM: client2.defaultIdentifier,
        DATA: "some string"})

    update = {
        'reason': "{}{} attribute is missing or not in proper format" \
            .format(commonError, DATA)}

    coros3 = [partial(checkReqNack, client1, node, req.reqId, update)
             for node in nodeSet]

    req, = client1.submit({
        TXN_TYPE: CREDIT,
        TARGET_NYM: client2.defaultIdentifier,
        DATA: {
            AMOUNT: -3
        }})

    update = {
        'reason': "{}{} must be present and should be a number greater than 0"\
                    .format(commonError, AMOUNT)}

    coros4 = [partial(checkReqNack, client1, node, req.reqId, update)
              for node in nodeSet]

    looper.run(eventuallyAll(*(coros1+coros2+coros3+coros4), totalTimeout=5))

    req, = client1.submit({
        TXN_TYPE: CREDIT,
        TARGET_NYM: client2.defaultIdentifier,
        DATA: {
            AMOUNT: 30
        }})
    looper.run(eventually(checkSufficientRepliesRecvd, client1.inBox,
                          req.reqId, 1,
                          retryWait=1, timeout=5))
    for n in nodeSet:  # type: Node
        opVerifier, = n.opVerifiers
        assert opVerifier.count == 1
