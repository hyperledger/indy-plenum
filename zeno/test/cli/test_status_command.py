from typing import Sequence

import pytest

from zeno.test.cli.helper import isHeadingToken, isNameToken


def checkNodeStatusToken(nodeName, tokens):
    headingToken, nameToken = tokens
    hToken, hMsg = headingToken

    # Appropriate heading token should be printed
    assert isHeadingToken(hToken)
    assert hMsg == "Status for node:"

    nToken, nMsg = nameToken
    # Appropriate name token should be printed
    assert isNameToken(nToken)
    assert nMsg == nodeName


def checkForNamedTokens(tokens, expectedNames):
    # Looking for the expected names in given tokens

    lookingForNames = set(expectedNames)

    for token, name in tokens:
        assert isNameToken(token)
        assert name in lookingForNames
        # Remove the name if token for that name is found
        lookingForNames.remove(name)

    assert len(lookingForNames) == 0


def checkNodeStatus(cli, nodeName, connectedNodes, connectedClients):
    # Depends on the fact the `status node <nodeName> was the last command`
    # The last printed token would be the new line token

    printedTokens = cli.printedTokens
    connectedClientTokens = []
    if not cli.clients:
        # The void message should be shown if there are no clients
        assert connectedClients == cli.voidMsg
        # The second last token would be the connected node names
        connectedNodeTokens = printedTokens[1]['tokens']
        # The third last token would be the status line
        nodeStatusTokens = printedTokens[2]['tokens']
    else:
        # If there are any clients then the clients would be the second last
        # token
        connectedClientTokens = printedTokens[1]['tokens']
        # The fourth last token would be the connected node names
        connectedNodeTokens = printedTokens[3]['tokens']
        # The fifth last token would be the status line
        nodeStatusTokens = printedTokens[4]['tokens']

    checkNodeStatusToken(nodeName, nodeStatusTokens)
    checkForNamedTokens(connectedNodeTokens, connectedNodes)
    if cli.clients:
        checkForNamedTokens(connectedClientTokens, connectedClients)


def testStatusAtCliStart(cli):
    """
    Testing `status` command at the start of cli when no nodes or clients
    are created
    """
    cli.enterCmd("status")
    printeds = cli.printeds
    nodeStatus = printeds[3]
    clientStatus = printeds[2]
    assert nodeStatus['msg'] == "No nodes are running. Try typing " \
                                "'new node <name>'."
    assert clientStatus['msg'] == "Clients: No clients are running. Try " \
                                  "typing 'new client <name>'."


@pytest.mark.xfail(reason="Status output changed. Need to update test "
                          "accordingly")
def testStatusAfterOneNodeCreated(cli, validNodeNames):
    """
    Testing `status` and `status node <nodeName>` command after one node is
    created
    """
    nodeName = validNodeNames[0]
    cli.enterCmd("new node {}".format(nodeName))
    # Let the node start up
    cli.looper.runFor(3)

    cli.enterCmd("status")
    startedNodeToken = cli.printedTokens[1]['tokens']
    printeds = cli.printeds
    clientStatus = printeds[2]
    checkForNamedTokens(startedNodeToken, (nodeName, ))
    assert clientStatus['msg'] == "Clients: No clients are running. Try " \
                                  "typing " \
                                  "'new client <name>'."

    cli.enterCmd("status node {}".format(nodeName))
    checkNodeStatusToken(nodeName, cli.lastPrintTokenArgs['tokens'])


@pytest.mark.xfail(reason="Status output changed. Need to update test "
                          "accordingly")
def testStatusAfterAllNodesUp(cli, validNodeNames, allNodesUp):
    # Checking the output after command `status`. Testing the pool status here
    cli.enterCmd("status")
    printeds = cli.printeds
    clientStatus = printeds[0]
    startedNodeNames = {p['msg'] for p in printeds[1:5]}
    nodeStatus = printeds[5]
    assert clientStatus['msg'] == "Clients: No clients are running. Try " \
                                  "typing " \
                                  "'new client <name>'."
    assert nodeStatus['msg'] == "The following nodes are up and running: "
    assert startedNodeNames == set(validNodeNames)

    for name in validNodeNames:
        # Checking the output after command `status node <name>`. Testing
        # the node status here
        cli.enterCmd("status node {}".format(name))
        otherNodeNames = (set(validNodeNames) - {name, })
        checkNodeStatus(cli,
                        name,
                        otherNodeNames,
                        cli.voidMsg)


@pytest.mark.xfail(reason="Status output changed. Need to update test "
                          "accordingly")
def testStatusAfterClientAdded(cli, validNodeNames, allNodesUp):
    clientName = "Joe"
    cli.enterCmd("new client {}".format(clientName))
    # Let the client get connected to the nodes
    cli.looper.runFor(3)

    for name in validNodeNames:
        # Checking the output after command `status node <name>`. Testing
        # the node status here after the client is connected
        cli.enterCmd("status node {}".format(name))
        otherNodeNames = (set(validNodeNames) - {name, })
        checkNodeStatus(cli,
                        name,
                        otherNodeNames,
                        {clientName, })