import types

from stp_core.loop.eventually import eventually
from plenum.common.util import getMaxFailures
from plenum.test.cli.helper import checkRequest
from plenum.test.helper import checkSufficientRepliesReceived


def testLogFiltering(cli, validNodeNames, createAllNodes):
    msg = '{"Hello": "There", "type": "greeting"}'
    client, wallet = checkRequest(cli, msg)

    x = client.handleOneNodeMsg

    def handleOneNodeMsg(self, wrappedMsg, excludeFromCli=None):
        return x(wrappedMsg, excludeFromCli=True)

    client.handleOneNodeMsg = types.MethodType(handleOneNodeMsg, client)
    client.nodestack.msgHandler = client.handleOneNodeMsg
    msg = '{"Hello": "Where", "type": "greeting"}'
    cli.enterCmd('client {} send {}'.format(client.name, msg))
    cli.looper.run(eventually(
        checkSufficientRepliesReceived,
        client.inBox,
        wallet._getIdData().lastReqId,
        getMaxFailures(len(cli.nodes)),
        retryWait=2,
        timeout=10))
    assert "got msg from node" not in cli.lastCmdOutput
