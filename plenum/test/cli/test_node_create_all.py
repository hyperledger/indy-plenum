from plenum.test.helper import checkPoolReady


def testNodeCreateAll(cli, validNodeNames, createAllNodes):
    # Check if all nodes are connected
    checkPoolReady(cli.looper, cli.nodes.values())

    # Check if all nodes are added
    assert len(cli.nodes) == len(validNodeNames)
    assert set(cli.nodes.keys()) == set(cli.nodeReg.keys())
