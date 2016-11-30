from plenum.common.util import randomString
from plenum.test.cli.helper import isErrorToken, checkNodeStarted
from plenum.test.test_node import checkPoolReady


def testNodeNames(cli, validNodeNames):
    """
    Test adding nodes with valid and invalid names. Also testing adding nodes
    with duplicate names
    """
    # Add nodes with valid names
    for i, nm in enumerate(validNodeNames):
        cli.enterCmd("new node {}".format(nm))
        # Count of cli.nodes should increase by 1
        assert len(cli.nodes) == (i + 1)
        checkNodeStarted(cli, nm)

    checkPoolReady(cli.looper, cli.nodes.values())

    # Create a node with a name of an already created node
    cli.enterCmd("new node {}".format(nm))
    msg = cli.lastPrintArgs['msg']
    # Appropriate error msg should be printed
    assert msg == "Node {} already exists.".format(nm)
    # Count of cli.nodes should not change
    assert len(cli.nodes) == 4

    randName = randomString(10)
    cli.enterCmd("new node {}".format(randName))
    args = cli.printedTokens[-1]
    token, msg = args['tokens'][0]

    # An error token should be printed
    assert isErrorToken(token)
    # Appropriate error msg should be printed
    assert msg == "Invalid node name '{}'. ".format(randName)

    # Count of cli.nodes should not change
    assert len(cli.nodes) == len(validNodeNames)
    # Node name should be in cli.nodes
    assert randName not in cli.nodes
