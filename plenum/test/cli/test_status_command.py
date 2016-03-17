import logging

import pytest

from plenum.common.util import getMaxFailures
from plenum.test.cli.helper import isNameToken


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
    cli.looper.runFor(3)

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
    assert "Node listener: {}:{}".format(node.nodestack.ha[0],
                                         node.nodestack.ha[1]) in msgs[1]['msg']
    assert "Client listener: {}:{}".format(node.clientstack.ha[0],
                                           node.clientstack.ha[1]) in msgs[2]['msg']
    assert "Status:" in msgs[3]['msg']
    assert "Connections:" in msgs[4]['msg']
    assert not msgs[4]['newline']
    assert msgs[5]['msg'] == '<none>'
    assert "Replicas: 2" in msgs[6]['msg']
    assert "Up time (seconds)" in msgs[8]['msg']
    assert "Clients: " in msgs[9]['msg']
    assert not msgs[9]['newline']


def testStatusAfterAllNodesUp(cli, validNodeNames, createAllNodes):
    # Checking the output after command `status`. Testing the pool status here
    # waiting here for 5 seconds, So that after creating a node the whole output is printed first.
    cli.looper.runFor(5)
    cli.enterCmd("status")
    cli.looper.runFor(1)
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
        if node.hasPrimary:
            msgs = list(reversed(cli.printeds[:10]))
            checkPrimaryLogs(node, msgs)
        else:
            msgs = list(reversed(cli.printeds[:10]))
            checkNonPrimaryLogs(node, msgs)

        checkForNamedTokens(cli.printedTokens[1], otherNodeNames)
        if cli.clients:
            checkForNamedTokens(cli.printedTokens[1], cli.voidMsg)


def testStatusAfterClientAdded(cli, validNodeNames, createAllNodes):
    # waiting here for 5 seconds, So that after creating a node the whole
    # output is printed first.
    cli.looper.runFor(5)
    clientName = "Joe"
    cli.enterCmd("new client {}".format(clientName))
    # Let the client get connected to the nodes
    cli.looper.runFor(5)

    for name in validNodeNames:
        # Checking the output after command `status node <name>`. Testing
        # the node status here after the client is connected
        cli.enterCmd("status node {}".format(name))
        otherNodeNames = (set(validNodeNames) - {name, })
        node = cli.nodes[name]
        if node.hasPrimary:
            try:
                msgs = list(reversed(cli.printeds[:9]))
                checkPrimaryLogs(node, msgs)
            except AssertionError:
                msgs = list(reversed(cli.printeds[:10]))
                checkPrimaryLogs(node, msgs)
        else:
            try:
                msgs = list(reversed(cli.printeds[:8]))
                checkNonPrimaryLogs(node, msgs)
            except AssertionError:
                msgs = list(reversed(cli.printeds[:9]))
                logging.info(">>>>> {}".format(msgs))
                checkPrimaryLogs(node, msgs)
            checkForNamedTokens(cli.printedTokens[3], otherNodeNames)
        if cli.clients:
            checkForNamedTokens(cli.printedTokens[1], {clientName, })


def checkPrimaryLogs(node, msgs):
    checkCommonLogs(node, msgs)
    assert "Replicas: 2" in msgs[5]['msg']
    # assert "(primary of " in msgs[6]['msg']
    assert "Up time (seconds)" in msgs[7]['msg']
    assert "Clients: " in msgs[8]['msg']
    assert not msgs[8]['newline']


def checkNonPrimaryLogs(node, msgs):
    checkCommonLogs(node, msgs)
    assert not msgs[4]['newline']
    assert not msgs[5]['newline']
    assert "Replicas: 2" in msgs[5]['msg']
    assert "Up time (seconds)" in msgs[7]['msg']
    assert "Clients: " in msgs[8]['msg']
    assert not msgs[8]['newline']


def checkNodeStatusToken(node, msgs):
    assert not msgs[4]['newline']
    assert msgs[5]['msg'] == '<none>'
    assert "Replicas: 2" in msgs[6]['msg']
    assert "Up time (seconds)" in msgs[7]['msg']
    assert "Clients: " in msgs[8]['msg']
    assert not msgs[8]['newline']


def checkCommonLogs(node, msgs):
    assert "Name: {}".format(node.name) in msgs[0]['msg']
    assert "Node listener: {}:{}".format(node.nodestack.ha[0],
                                         node.nodestack.ha[1]) in msgs[1]['msg']
    assert "Client listener: {}:{}".format(node.clientstack.ha[0],
                                           node.clientstack.ha[1]) in msgs[2]['msg']
    assert "Status:" in msgs[3]['msg']
    assert "Connections:" in msgs[4]['msg']
