from plenum.common.util import randomString
from plenum.test.cli.helper import isErrorToken, waitNodeStarted
from plenum.test.test_node import checkPoolReady


def addNodes(be, do, cli, validNodeNames):
    # Add nodes with valid names
    be(cli)
    for i, nm in enumerate(validNodeNames):
        do("new node {}".format(nm))
        waitNodeStarted(cli, nm)


def testNodeNames(be, do, cli, validNodeNames):
    """
    Test adding nodes with valid and invalid names. Also testing adding nodes
    with duplicate names
    """
    addNodes(be, do, cli, validNodeNames)
    checkPoolReady(cli.looper, cli.nodes.values())
    lastNodeName = validNodeNames[-1]

    # Create a node with a name of an already created node
    be(cli)
    do("new node {}".format(lastNodeName), expect=[
        "Node {} already exists.".format(lastNodeName)])
    assert len(cli.nodes) == 4

    # Create a node with invalid name
    randName = randomString(10)
    do("new node {}".format(randName), expect=[
        "Invalid node name '{}'. ".format(randName)])
    args = cli.printedTokens[-1]
    token, _ = args['tokens'][0]
    # An error token should be printed
    assert isErrorToken(token)
    # Count of cli.nodes should not change
    assert len(cli.nodes) == len(validNodeNames)
    # Node name should NOT be in cli.nodes
    assert randName not in cli.nodes


def testCreateNodeWhenClientExistsWithoutKey(be, do, cli, validNodeNames):
    clientName = "testc1"
    be(cli)
    do("new client {}".format(clientName), expect=[
        "Active client set to {}".format(clientName)])
    do("new node {}".format(validNodeNames[0]), expect=[
        "No key present in wallet"], within=2)


def testCreateNodeWhenClientExistsWithKey(be, do, cli, validNodeNames):
    clientName = "testc2"
    be(cli)
    do("new client {}".format(clientName), expect=[
        "Active client set to {}".format(clientName)])
    do("new key", expect=["Current DID set to "])
    addNodes(be, do, cli, validNodeNames)
