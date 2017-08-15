import pytest

from stp_core.loop.eventually import eventually
from plenum.common.util import getMaxFailures
from plenum.test.cli.helper import isNameToken, \
    waitNodeStarted, \
    checkActiveIdrPrinted
from plenum.test import waits
from plenum.common import util
from plenum.test.cli.helper import waitClientConnected


def checkForNamedTokens(printedTokens, expectedNames):
    # Looking for the expected names in given tokens
    lookingForNames = set(expectedNames)

    for printedToken in printedTokens['tokens']:
        assert isNameToken(printedToken[0])
        assert printedToken[1] in lookingForNames
        # Remove the name if token for that name is found
        lookingForNames.remove(printedToken[1])

    assert len(lookingForNames) == 0


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


def testStatusAfterOneNodeCreated(cli, validNodeNames):
    """
    Testing `status` and `status node <nodeName>` command after one node is
    created
    """
    nodeName = validNodeNames[0]
    cli.enterCmd("new node {}".format(nodeName))
    # Let the node start up
    waitNodeStarted(cli, nodeName)

    cli.enterCmd("status")
    startedNodeToken = cli.printedTokens[1]
    printeds = cli.printeds
    clientStatus = printeds[2]
    checkForNamedTokens(startedNodeToken, (nodeName,))
    assert clientStatus['msg'] == "Clients: No clients are running. Try " \
                                  "typing " \
                                  "'new client <name>'."
    cli.enterCmd("status node {}".format(nodeName))
    msgs = list(reversed(cli.printeds[:11]))
    node = cli.nodes[nodeName]
    assert "Name: {}".format(node.name) in msgs[0]['msg']
    assert "Node listener: 0.0.0.0:{}".format(node.nodestack.ha[1]) in \
           msgs[1]['msg']
    assert "Client listener: 0.0.0.0:{}".format(node.clientstack.ha[1]) \
           in msgs[2]['msg']
    assert "Status:" in msgs[3]['msg']
    assert "Connections:" in msgs[4]['msg']
    assert not msgs[4]['newline']
    assert msgs[5]['msg'] == '<none>'
    assert "Replicas: 2" in msgs[6]['msg']
    assert "Up time (seconds)" in msgs[8]['msg']
    assert "Clients: " in msgs[9]['msg']
    assert not msgs[9]['newline']


# This test fails intermittently when the whole test package is run, fails
# because the fixture `createAllNodes` fails, the relevant bug is
# https://www.pivotaltracker.com/story/show/126771175
@pytest.mark.skip(reason="SOV-548. "
                         "Intermittently fails due to a bug mentioned "
                         "in the above comment")
def testStatusAfterAllNodesUp(cli, validNodeNames, createAllNodes):
    # Checking the output after command `status`. Testing the pool status here
    cli.enterCmd("status")
    printeds = cli.printeds
    clientStatus = printeds[4]
    fValue = printeds[3]['msg']
    assert clientStatus['msg'] == "Clients: No clients are running. Try " \
                                  "typing " \
                                  "'new client <name>'."
    assert fValue == "f-value (number of possible faulty nodes): {}".format(
        getMaxFailures(len(validNodeNames)))

    for name in validNodeNames:
        # Checking the output after command `status node <name>`. Testing
        # the node status here
        cli.enterCmd("status node {}".format(name))
        cli.looper.runFor(1)
        otherNodeNames = (set(validNodeNames) - {name, })
        node = cli.nodes[name]
        cliLogs = list(cli.printeds)
        if node.hasPrimary:
            checkPrimaryLogs(node, cliLogs)
        else:
            checkNonPrimaryLogs(node, cliLogs)
            checkForNamedTokens(cli.printedTokens[1], otherNodeNames)
        if cli.clients:
            checkForNamedTokens(cli.printedTokens[1], cli.voidMsg)


# This test fails intermittently when the whole test package is run, fails
# because the fixture `createAllNodes` fails
@pytest.mark.skip(reason="SOV-549. "
                         "Intermittently fails due to a bug mentioned "
                         "in the above comment")
def testStatusAfterClientAdded(cli, validNodeNames, createAllNodes):
    clientName = "Joe"
    cli.enterCmd("new client {}".format(clientName))

    connectionTimeout = \
        waits.expectedClientToPoolConnectionTimeout(len(validNodeNames))

    waitClientConnected(cli, validNodeNames, clientName)

    cli.enterCmd("new key")
    cli.enterCmd("status client {}".format(clientName))
    cli.looper.run(eventually(checkActiveIdrPrinted, cli,
                              retryWait=1, timeout=connectionTimeout))

    for name in validNodeNames:
        # Checking the output after command `status node <name>`. Testing
        # the node status here after the client is connected
        cli.enterCmd("status node {}".format(name))
        otherNodeNames = (set(validNodeNames) - {name, })
        node = cli.nodes[name]
        client = cli.clients[clientName]
        cliLogs = list(cli.printeds)
        if node.hasPrimary:
            checkPrimaryLogs(node, cliLogs)
        else:
            checkNonPrimaryLogs(node, cliLogs)
            checkForNamedTokens(cli.printedTokens[3], otherNodeNames)
        if cli.clients:
            checkForNamedTokens(cli.printedTokens[1], {client.stackName, })


def checkPrimaryLogs(node, msgs):
    checkCommonLogs(node, msgs)
    shouldBePresent = ["Replicas: 2",
                       "(primary of ",
                       "Up time (seconds)",
                       "Clients: "
                       ]
    keys = [msg['msg'] for msg in msgs]

    for sbp in shouldBePresent:
        assert any([sbp in key for key in keys])


def checkNonPrimaryLogs(node, msgs):
    checkCommonLogs(node, msgs)
    shouldBePresent = ["Replicas: 2",
                       "Up time (seconds)",
                       "Clients: "
                       ]
    keys = [msg['msg'] for msg in msgs]

    for sbp in shouldBePresent:
        assert any([sbp in key for key in keys])


def checkCommonLogs(node, msgs):
    shouldBePresent = ["Name: {}".format(node.name),
                       "Node listener: 0.0.0.0:{}".format(
                           node.nodestack.ha[1]),
                       "Client listener: 0.0.0.0:{}".format(
                           node.clientstack.ha[1]),
                       "Status:",
                       "Connections:"
                       ]
    keys = [msg['msg'] for msg in msgs]

    for sbp in shouldBePresent:
        assert any([sbp in key for key in keys])
