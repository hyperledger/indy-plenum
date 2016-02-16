from zeno.common.util import getMaxFailures
from zeno.test.eventually import eventually
from zeno.test.helper import checkSufficientRepliesRecvd


def testClientRequest(cli, validNodeNames, looper, allNodesUp):
    """
    Test client sending request and checking its status
    """
    cName = "Joe"
    cli.enterCmd("new client {}".format(cName))
    # Let client connect to the nodes
    looper.runFor(3)
    # Send request to all nodes
    cli.enterCmd('client {} send {}'.format(cName, '{"Hello"}'))
    client = cli.clients[cName]
    f = getMaxFailures(len(cli.nodes))
    # Ensure client gets back the replies
    looper.run(eventually(
                checkSufficientRepliesRecvd,
                client.inBox,
                client.lastReqId,
                f,
                retryWait=2,
                timeout=30))

    # Ensure the cli shows appropriate output
    cli.enterCmd('client {} show {}'.format(cName, client.lastReqId))
    assert cli.lastPrintArgs['msg'] == "Status: CONFIRMED"