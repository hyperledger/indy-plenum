import types

import pytest
from plenum.common.util import getMaxFailures
from plenum.test.cli.helper import checkRequest
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd


def testLogFiltering(cli, validNodeNames, createAllNodes):
    msg = '{"Hello": "There"}'
    client, wallet = checkRequest(cli, msg)
    # client = next(iter(cli.clients.values()))

    x = client.handleOneNodeMsg

    def handleOneNodeMsg(self, wrappedMsg, excludeFromCli=None):
        return x(wrappedMsg, excludeFromCli=True)

    client.handleOneNodeMsg = types.MethodType(handleOneNodeMsg, client)
    client.nodestack.msgHandler = client.handleOneNodeMsg
    msg = '{"Hello": "Where"}'
    cli.enterCmd('client {} send {}'.format(client.name, msg))
    cli.looper.run(eventually(
        checkSufficientRepliesRecvd,
        client.inBox,
        wallet._getIdData().lastReqId,
        getMaxFailures(len(cli.nodes)),
        retryWait=2,
        timeout=10))
    assert "got msg from node" not in cli.lastCmdOutput
