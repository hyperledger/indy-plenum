from zeno.common.util import getMaxFailures
from zeno.test.eventually import eventually
from zeno.test.helper import checkSufficientRepliesRecvd


def testClientRequest(cli, validNodeNames, cliLooper, createAllNodes):
    """
    Test client sending request and checking reply and status
    """
    cName = "Joe"
    cli.enterCmd("new client {}".format(cName))
    # Let client connect to the nodes
    cliLooper.runFor(3)
    # Send request to all nodes
    cli.enterCmd('client {} send {}'.format(cName, '{"Hello"}'))
    client = cli.clients[cName]
    f = getMaxFailures(len(cli.nodes))
    # Ensure client gets back the replies
    cliLooper.run(eventually(
                checkSufficientRepliesRecvd,
                client.inBox,
                client.lastReqId,
                f,
                retryWait=2,
                timeout=30))

    txn, status = client.getReply(client.lastReqId)

    # Ensure the cli shows appropriate output
    cli.enterCmd('client {} show {}'.format(cName, client.lastReqId))
    printeds = cli.printeds
    printedReply = printeds[1]
    printedStatus = printeds[0]
    assert printedReply['msg'] == "Reply for the request: {{'txnId': '{}" \
                                  "'}}".format(txn['txnId'])
    assert printedStatus['msg'] == "Status: {}".format(status)