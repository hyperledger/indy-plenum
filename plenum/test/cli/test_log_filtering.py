import types

from plenum.test.cli.helper import checkRequest
from plenum.test.helper import waitForSufficientRepliesForRequests
from plenum.common.request import Request


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

    request = Request(identifier=wallet.defaultId,
                      reqId=wallet._getIdData().lastReqId)

    waitForSufficientRepliesForRequests(cli.looper, client,
                                        requests=[request])

    assert "got msg from node" not in cli.lastCmdOutput
